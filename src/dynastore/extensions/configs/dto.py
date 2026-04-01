#    Copyright 2026 FAO
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

"""
Configuration API — Data Transfer Objects.

Typed request/response models for the ``/configs`` extension.
All models carry ``json_schema_extra`` examples that show PostgreSQL-first
defaults so users can bootstrap a working catalog without guessing.
"""

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums — well-known plugin identifiers
# ---------------------------------------------------------------------------


class WellKnownPlugin(StrEnum):
    """Discoverable plugin identifiers registered by core modules.

    Use ``GET /configs/plugins`` for the live list; this enum documents
    the most common ones for IDE auto-complete and OpenAPI examples.
    """

    COLLECTION = "collection"
    STAC = "stac"
    STORAGE_COLLECTIONS = "storage:collections"
    STORAGE_ASSETS = "storage:assets"
    # Deprecated aliases — kept for backward compatibility
    ROUTING = "routing"
    ROUTING_ASSETS = "routing_assets"
    DRIVER_POSTGRESQL = "driver:postgresql"
    DRIVER_POSTGRESQL_ASSETS = "driver:postgresql_assets"
    DRIVER_ELASTICSEARCH = "driver:elasticsearch"
    DRIVER_ELASTICSEARCH_ASSETS = "driver:elasticsearch_assets"
    DRIVER_DUCKDB = "driver:duckdb"
    ELASTICSEARCH = "elasticsearch"
    TILES = "tiles"
    TILES_PRESEED = "tiles_preseed"
    TASKS = "tasks"
    FEATURES = "features"
    WFS = "wfs"
    INGESTION = "ingestion"
    SECURITY = "security"


# ---------------------------------------------------------------------------
# Shared sub-models
# ---------------------------------------------------------------------------


class ConfigLevel(StrEnum):
    """Hierarchy tier from which a configuration was resolved."""

    COLLECTION = "collection"
    CATALOG = "catalog"
    PLATFORM = "platform"
    DEFAULT = "default"


class PluginSchemaInfo(BaseModel):
    """Schema metadata for a registered plugin."""

    description: str = Field(..., description="Human-readable description from the model docstring.")
    schema_: Dict[str, Any] = Field(..., alias="schema", description="Full JSON Schema of the config model.")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Response DTOs
# ---------------------------------------------------------------------------


class ConfigEntry(BaseModel):
    """A single configuration entry as returned by list/search endpoints."""

    plugin_id: str = Field(..., description="Unique plugin identifier.", examples=["driver:postgresql"])
    config_data: Dict[str, Any] = Field(..., description="The stored configuration payload.")
    updated_at: Optional[datetime] = Field(None, description="Last modification timestamp (UTC).")


class ConfigListResponse(BaseModel):
    """Paginated list of configuration entries."""

    items: List[ConfigEntry] = Field(default_factory=list)
    total: int = Field(0, ge=0, description="Total items matching the query.")
    limit: int = Field(10, ge=1, le=1000)
    offset: int = Field(0, ge=0)


class EffectiveConfigResponse(BaseModel):
    """The resolved (effective) configuration for a plugin at a given scope.

    The ``resolved_from`` field tells the caller which hierarchy tier
    supplied the value, so they know whether it is an explicit override
    or a fallback default.
    """

    plugin_id: str = Field(..., examples=["driver:postgresql"])
    config: Dict[str, Any] = Field(..., description="Effective configuration payload.")
    resolved_from: Optional[ConfigLevel] = Field(
        None,
        description="Hierarchy tier that provided this configuration.",
    )


class DriverInfo(BaseModel):
    """Metadata about a registered storage driver."""

    driver_id: str = Field(..., description="Unique driver identifier.")
    domain: str = Field(
        ...,
        description="Domain this driver serves: 'collections' or 'assets'.",
        examples=["collections", "assets"],
    )
    capabilities: List[str] = Field(
        default_factory=list,
        description="What the driver can do (Capability constants: read, write, fulltext, ...).",
    )
    driver_capabilities: List[str] = Field(
        default_factory=list,
        description="How the driver operates (DriverCapability: SYNC, ASYNC, TRANSACTIONAL, ...).",
    )
    supported_operations: List[str] = Field(
        default_factory=list,
        description="Operations this driver supports, derived from its capabilities.",
    )
    supported_hints: List[str] = Field(
        default_factory=list,
        description="Hints this driver accepts in routing config entries.",
    )
    preferred_for: List[str] = Field(
        default_factory=list,
        description="Hints this driver is optimized for (used for auto-selection).",
    )


class DriverListResponse(BaseModel):
    """All registered storage drivers with their capabilities and supported hints."""

    drivers: List[DriverInfo] = Field(default_factory=list)


class PluginListResponse(BaseModel):
    """List of registered plugin IDs (without schemas)."""

    plugins: List[str] = Field(
        ...,
        examples=[
            [
                "collection",
                "stac",
                "storage:collections",
                "storage:assets",
                "driver:postgresql",
                "driver:postgresql_assets",
                "tiles",
                "tasks",
                "features",
                "wfs",
            ]
        ],
    )


class PluginSchemaResponse(BaseModel):
    """All registered plugins with their JSON Schemas."""

    plugins: Dict[str, PluginSchemaInfo]


# ---------------------------------------------------------------------------
# Request DTOs — with PostgreSQL-first examples
# ---------------------------------------------------------------------------

# ---- Routing ----

_ROUTING_PG_EXAMPLE: Dict[str, Any] = {
    "summary": "PostgreSQL read/write (default)",
    "description": (
        "Routes both WRITE and READ operations to the ``postgresql`` driver. "
        "This is the simplest setup and suitable for most collections."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
        },
    },
}

_ROUTING_PG_ES_EXAMPLE: Dict[str, Any] = {
    "summary": "PostgreSQL write + Elasticsearch search (parallel sync)",
    "description": (
        "Fan-out writes to both PG (sync) and ES (async); reads go to PG, "
        "searches go to ES for full-text / geo queries.  "
        "ES writes fire after PG commit (fire-and-forget)."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [
                {"driver_id": "postgresql", "hints": [], "on_failure": "fatal", "write_mode": "sync"},
                {"driver_id": "elasticsearch", "hints": [], "on_failure": "warn", "write_mode": "async"},
            ],
            "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "SEARCH": [{"driver_id": "elasticsearch", "hints": ["search"], "on_failure": "fatal"}],
        },
    },
}

_ROUTING_ES_ONLY_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch only (read/write/search)",
    "description": (
        "Routes all operations to Elasticsearch. "
        "Use this when ES is the sole collection store (no PG backend)."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [{"driver_id": "elasticsearch", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "elasticsearch", "hints": [], "on_failure": "fatal"}],
            "SEARCH": [{"driver_id": "elasticsearch", "hints": [], "on_failure": "fatal"}],
        },
    },
}

_ROUTING_DUCKDB_ES_EXAMPLE: Dict[str, Any] = {
    "summary": "DuckDB write/read + Elasticsearch search",
    "description": (
        "Writes go to DuckDB (Parquet files); searches are served by "
        "Elasticsearch.  Suitable for analytical / batch-ingested collections "
        "that need fast full-text and geo search."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [
                {"driver_id": "duckdb", "hints": [], "on_failure": "fatal", "write_mode": "sync"},
                {"driver_id": "elasticsearch", "hints": [], "on_failure": "warn", "write_mode": "async"},
            ],
            "READ": [{"driver_id": "duckdb", "hints": [], "on_failure": "fatal"}],
            "SEARCH": [{"driver_id": "elasticsearch", "hints": ["search"], "on_failure": "fatal"}],
        },
    },
}

_ROUTING_PG_ES_ICEBERG_EXAMPLE: Dict[str, Any] = {
    "summary": "PG + ES + Iceberg — triple write (parallel sync)",
    "description": (
        "Writes to PostgreSQL and Iceberg in parallel (sync), then fires "
        "Elasticsearch indexing asynchronously.  Reads from PG, searches "
        "on ES.  If any sync writer fails, all sync writers that support "
        "TRANSACTIONAL capability are compensated (rolled back)."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [
                {"driver_id": "postgresql", "hints": [], "on_failure": "fatal", "write_mode": "sync"},
                {"driver_id": "iceberg", "hints": [], "on_failure": "fatal", "write_mode": "sync"},
                {"driver_id": "elasticsearch", "hints": [], "on_failure": "warn", "write_mode": "async"},
            ],
            "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "SEARCH": [{"driver_id": "elasticsearch", "hints": ["search"], "on_failure": "fatal"}],
        },
    },
}

_ROUTING_GEOPARQUET_EXAMPLE: Dict[str, Any] = {
    "summary": "GeoParquet collection — PG write + DuckDB read + ES search",
    "description": (
        "The GeoParquet use case: a parquet file is registered as a collection "
        "asset and imported via the STAC Items API (writes to PostgreSQL).  "
        "Custom pipelines read directly from the parquet via DuckDB.  "
        "OpenSearch serves full-text and spatial search queries.  "
        "The OGC Features API always reads from PostgreSQL regardless of "
        "this routing config — the READ entry is for explicit driver access "
        "via ``get_driver('READ', ...)`` in import scripts and enrichment tasks."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "duckdb", "hints": [], "on_failure": "fatal"}],
            "SEARCH": [{"driver_id": "elasticsearch", "hints": [], "on_failure": "fatal"}],
        },
    },
}

_ROUTING_ASSETS_ES_EXAMPLE: Dict[str, Any] = {
    "summary": "Asset routing — Elasticsearch only",
    "description": (
        "Routes asset WRITE and READ to the Elasticsearch asset driver. "
        "Use when the asset catalog is stored entirely in ES."
    ),
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [{"driver_id": "elasticsearch", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "elasticsearch", "hints": [], "on_failure": "fatal"}],
        },
    },
}

# ---- Driver: PostgreSQL ----

_DRIVER_PG_MINIMAL_EXAMPLE: Dict[str, Any] = {
    "summary": "PostgreSQL driver — minimal (all defaults)",
    "description": (
        "Uses default sidecars (geometries + attributes) with no partitioning. "
        "Suitable for most vector collections under ~10M features."
    ),
    "value": {
        "enabled": True,
        "collection_type": "VECTOR",
        "sidecars": [
            {
                "sidecar_type": "geometries",
                "enabled": True,
                "target_srid": 4326,
                "target_dimension": "force_2d",
                "geom_column": "geom",
                "bbox_column": "bbox_geom",
                "invalid_geom_policy": "attempt_fix",
                "srid_mismatch_policy": "transform",
            },
            {
                "sidecar_type": "attributes",
                "enabled": True,
                "storage_mode": "automatic",
                "enable_external_id": True,
                "enable_asset_id": True,
                "versioning_behavior": "UPDATE_EXISTING_VERSION",
            },
        ],
        "partitioning": {"enabled": False, "partition_keys": []},
    },
}

_DRIVER_PG_PARTITIONED_EXAMPLE: Dict[str, Any] = {
    "summary": "PostgreSQL driver — H3 partitioned",
    "description": (
        "Enables H3-based spatial partitioning for large collections. "
        "The geometry sidecar contributes the ``h3_res4`` partition key."
    ),
    "value": {
        "enabled": True,
        "collection_type": "VECTOR",
        "sidecars": [
            {
                "sidecar_type": "geometries",
                "enabled": True,
                "target_srid": 4326,
                "target_dimension": "force_2d",
                "geom_column": "geom",
                "bbox_column": "bbox_geom",
                "invalid_geom_policy": "attempt_fix",
                "srid_mismatch_policy": "transform",
                "h3_resolutions": [4],
                "partition_strategy": "BY_H3",
                "partition_resolution": 4,
            },
            {
                "sidecar_type": "attributes",
                "enabled": True,
                "storage_mode": "automatic",
                "enable_external_id": True,
                "enable_asset_id": True,
                "versioning_behavior": "UPDATE_EXISTING_VERSION",
            },
        ],
        "partitioning": {"enabled": True, "partition_keys": ["h3_res4"]},
    },
}

# ---- Driver: Elasticsearch ----
#
# The ES driver uses the stac-fastapi-elasticsearch-opensearch (SFEOS) library
# (``DatabaseLogic``) to write STAC items, collections, and catalogs.
# This makes the indexes natively readable by an external SFEOS application
# running in read-only mode on the same ES/OpenSearch cluster.
#
# Convention: SFEOS-compatible by default.
#
# INDEX NAMING (SFEOS convention):
#   Items:       {STAC_ITEMS_INDEX_PREFIX}{collection_id}   (default: items_{cid})
#   Collections: {STAC_COLLECTIONS_INDEX}                   (default: collections)
#   Catalogs:    stored alongside collections (type=Catalog)
#
# SFEOS env vars (shared by both DynaStore and the external SFEOS app):
#   STAC_ITEMS_INDEX_PREFIX  = "items_"       — per-collection item indexes
#   STAC_COLLECTIONS_INDEX   = "collections"  — shared collections/catalogs index
#   ES_HOST / ES_PORT / ES_USE_SSL / ES_VERIFY_CERTS / ES_API_KEY — connection
#
# Additional DynaStore-only indexes:
#   {ES_INDEX_PREFIX}-geoid-{catalog_id}   — obfuscated (geoid-only) per catalog
#   {ES_INDEX_PREFIX}-assets-{catalog_id}  — asset metadata per catalog
#
# SFEOS read-only frontend env vars (ENABLE_CATALOGS_ROUTE=true):
#   ENABLE_CATALOGS_ROUTE=true              — enables /catalogs endpoint
#   ENABLE_TRANSACTIONS_EXTENSIONS=false    — read-only mode
#   ENABLE_COLLECTIONS_SEARCH=true          — collection-level search
#   STAC_FASTAPI_LANDING_PAGE_ID=my-api     — landing page identifier
#   STAC_FASTAPI_RATE_LIMIT=100/minute      — optional rate limiting
#   SENTRY_ENABLE=false                     — optional Sentry monitoring

_DRIVER_ES_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch driver — SFEOS-compatible (default)",
    "description": (
        "Default SFEOS-compatible index layout.  Each collection gets its own "
        "index: ``items_{collection_id}``.  Collections and catalogs go into "
        "a shared ``collections`` index.  "
        "An external stac-fastapi-elasticsearch-opensearch instance can read "
        "these indexes as a read-only STAC API by pointing to the same cluster "
        "with ENABLE_CATALOGS_ROUTE=true, ENABLE_TRANSACTIONS_EXTENSIONS=false."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC"],
        "index_prefix": "items_",
        "mapping": {},
    },
}

_DRIVER_ES_RASTER_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch driver — raster / Earth Observation collection",
    "description": (
        "SFEOS-compatible per-collection indexes with mapping extensions "
        "for Earth Observation data: ``eo:bands`` (nested for per-band search), "
        "``eo:cloud_cover``, ``sat:*`` orbit fields.  "
        "DynaStore writes raster STAC items; an external SFEOS instance serves "
        "them as a read-only OGC STAC API with full spatial/temporal/band "
        "filtering, aggregation, free-text search, and collection search."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC"],
        "index_prefix": "items_",
        "mapping": {
            "properties": {
                "geometry": {"type": "geo_shape"},
                "bbox": {"type": "float"},
                "properties": {
                    "properties": {
                        "datetime": {"type": "date"},
                        "start_datetime": {"type": "date"},
                        "end_datetime": {"type": "date"},
                        "eo:cloud_cover": {"type": "float"},
                        "eo:bands": {
                            "type": "nested",
                            "properties": {
                                "name": {"type": "keyword"},
                                "common_name": {"type": "keyword"},
                                "center_wavelength": {"type": "float"},
                            },
                        },
                        "sat:absolute_orbit": {"type": "integer"},
                        "sat:relative_orbit": {"type": "integer"},
                    }
                },
            }
        },
    },
}

_DRIVER_ES_CUSTOM_MAPPING_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch driver — custom text analyzer",
    "description": (
        "Custom text analyzer on ``description`` for improved full-text search.  "
        "The SFEOS frontend will use the same mapping for query parsing."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC"],
        "index_prefix": "items_",
        "mapping": {
            "properties": {
                "geometry": {"type": "geo_shape"},
                "properties.description": {
                    "type": "text",
                    "analyzer": "standard",
                },
            }
        },
    },
}

# ---- Driver: Elasticsearch Assets ----
#
# Asset indexes are always per-catalog: {ES_INDEX_PREFIX}-assets-{catalog_id}.
#
# This is the right granularity because:
#   - Asset counts are 1–3 orders of magnitude smaller than feature counts
#   - Per-catalog isolation is sufficient for access control and lifecycle
#   - Per-collection splitting would create too many tiny indexes with
#     poor shard efficiency
#   - Assets within a catalog are filtered by ``collection_id`` at query time

_DRIVER_ES_ASSETS_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch asset driver — per-catalog index (default)",
    "description": (
        "Stores asset metadata in per-catalog ES indexes: "
        "``{ES_INDEX_PREFIX}-assets-{catalog_id}``.  "
        "Each catalog gets its own index for tenant isolation.  "
        "Assets are filtered by ``collection_id`` at query time — "
        "per-collection splitting is unnecessary because asset volumes "
        "are far smaller than feature volumes."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC"],
        "index_prefix": "assets_",
    },
}

# ---- Driver: DuckDB ----

_DRIVER_DUCKDB_PARQUET_EXAMPLE: Dict[str, Any] = {
    "summary": "DuckDB driver — Parquet files",
    "description": (
        "Reads from a glob of Parquet files.  DuckDB handles predicate "
        "push-down and column pruning automatically.  Ideal for large "
        "analytical datasets that are batch-ingested."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC", "BATCH"],
        "format": "parquet",
        "path": "gs://my-bucket/collections/*/items/*.parquet",
    },
}

_DRIVER_DUCKDB_CSV_EXAMPLE: Dict[str, Any] = {
    "summary": "DuckDB driver — CSV with separate write path",
    "description": (
        "Reads CSV files from one location and writes to a separate "
        "path (e.g. a local SQLite staging area).  Useful for ETL "
        "workflows where source data is CSV."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC", "BATCH"],
        "format": "csv",
        "path": "/data/imports/*.csv",
        "write_path": "/data/staging/collection.sqlite",
        "write_format": "sqlite",
    },
}

_DRIVER_DUCKDB_GEOPARQUET_EXAMPLE: Dict[str, Any] = {
    "summary": "DuckDB driver — GeoParquet collection asset (read-only)",
    "description": (
        "Points DuckDB at a single GeoParquet file registered as a "
        "collection asset.  DuckDB's spatial extension converts WKB "
        "geometry to GeoJSON at query time and supports ST_Intersects "
        "spatial filtering.  No write_path — the parquet is read-only.  "
        "Pair with routing READ->duckdb + WRITE->postgresql to import "
        "features via the STAC Items API while keeping direct parquet "
        "reads available for enrichment pipelines."
    ),
    "value": {
        "enabled": True,
        "capabilities": ["ASYNC", "BATCH"],
        "format": "parquet",
        "path": "/data/countries.parquet",
    },
}

# ---- Elasticsearch catalog config ----
#
# Per-catalog ES settings managed via plugin_id "elasticsearch".
# Controls indexing mode (standard vs obfuscated) at the catalog level.
#
# Standard mode: items are fully indexed into the SFEOS-compatible
# per-collection indexes (``items_{collection_id}``) with geometry,
# attributes, and all STAC metadata.  An external SFEOS app can serve
# them transparently.
#
# Obfuscated mode: items are indexed into a separate per-catalog index
# (``{ES_INDEX_PREFIX}-geoid-{catalog_id}``) containing only the geoid
# UUID.  A DENY policy blocks all_users GET access.  This index is NOT
# served by the external SFEOS app — it is DynaStore-only.

_ES_CATALOG_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch catalog — standard mode (SFEOS-readable)",
    "description": (
        "Default catalog-level ES config.  ``obfuscated=false`` means items "
        "are fully indexed with geometry, attributes, and all STAC metadata "
        "into SFEOS-compatible per-collection indexes.  "
        "An external SFEOS instance (with ENABLE_CATALOGS_ROUTE=true, "
        "ENABLE_TRANSACTIONS_EXTENSIONS=false) can serve this catalog's "
        "items as a read-only STAC API."
    ),
    "value": {"enabled": True, "obfuscated": False},
}

_ES_CATALOG_OBFUSCATED_EXAMPLE: Dict[str, Any] = {
    "summary": "Elasticsearch catalog — obfuscated mode (DynaStore-only)",
    "description": (
        "Enables obfuscated indexing into a per-catalog index: "
        "``{ES_INDEX_PREFIX}-geoid-{catalog_id}``.  Only the geoid UUID, "
        "catalog_id, and collection_id are stored — no geometry, no "
        "attributes, no spatial search.  GET access is denied to all_users "
        "via a DENY policy.  Toggling triggers an automatic full catalog "
        "reindex.  This index is NOT visible to the external SFEOS app.  "
        "Use for sensitive catalogs where item existence must be hidden."
    ),
    "value": {"enabled": True, "obfuscated": True},
}

# ---- STAC ----

_STAC_MINIMAL_EXAMPLE: Dict[str, Any] = {
    "summary": "STAC — basic vector collection",
    "description": "Enables core STAC with asset tracking in DIRECT mode (no proxy).",
    "value": {
        "enabled": True,
        "enabled_extensions": [],
        "asset_tracking": {"enabled": True, "access_mode": "DIRECT"},
        "item_assets": {},
        "summaries": {},
        "providers": [],
    },
}

_STAC_DATACUBE_EXAMPLE: Dict[str, Any] = {
    "summary": "STAC — datacube with temporal dimension",
    "description": "Configures a temporal datacube dimension sourced from an attribute scan.",
    "value": {
        "enabled": True,
        "enabled_extensions": ["datacube"],
        "cube_dimensions": {
            "time": {
                "type": "temporal",
                "description": "Observation date",
                "extent": ["2000-01-01T00:00:00Z", None],
                "dynamic_source": {
                    "type": "attribute_scan",
                    "target_attribute": "datetime",
                },
            },
            "x": {"type": "spatial", "axis": "x", "reference_system": 4326},
            "y": {"type": "spatial", "axis": "y", "reference_system": 4326},
        },
        "asset_tracking": {"enabled": True, "access_mode": "DIRECT"},
    },
}

# ---- Collection ----

_COLLECTION_EXAMPLE: Dict[str, Any] = {
    "summary": "Collection config — structural (open schema)",
    "description": "Base collection config. Extra fields are allowed for forward-compat.",
    "value": {"enabled": True},
}

# ---- Routing Assets ----

_ROUTING_ASSETS_PG_EXAMPLE: Dict[str, Any] = {
    "summary": "Asset routing — PostgreSQL (default)",
    "description": "Routes asset WRITE and READ to the PostgreSQL asset driver.",
    "value": {
        "enabled": True,
        "operations": {
            "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
            "READ": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
        },
    },
}

# ---- Tiles ----

_TILES_EXAMPLE: Dict[str, Any] = {
    "summary": "Tiles — WebMercator, zoom 0-12",
    "description": "Default MVT tile configuration with on-demand caching.",
    "value": {
        "enabled": True,
        "min_zoom": 0,
        "max_zoom": 12,
        "supported_tms_ids": ["WebMercatorQuad"],
        "cache_on_demand": True,
        "simplification_algorithm": "topology_preserving",
    },
}

# ---- Features ----

_FEATURES_EXAMPLE: Dict[str, Any] = {
    "summary": "Features — OGC Features defaults",
    "description": "OGC API Features with bucket storage priority.",
    "value": {
        "enabled": True,
        "cache_on_demand": False,
        "storage_priority": ["bucket"],
    },
}

# ---- Tasks ----

_TASKS_EXAMPLE: Dict[str, Any] = {
    "summary": "Tasks — default polling",
    "description": "Task queue configuration with 30-second fallback poll.",
    "value": {"enabled": True, "queue_poll_interval": 30.0},
}


# ---------------------------------------------------------------------------
# Per-plugin example registry
# ---------------------------------------------------------------------------

PLUGIN_EXAMPLES: Dict[str, List[Dict[str, Any]]] = {
    WellKnownPlugin.STORAGE_COLLECTIONS: [
        _ROUTING_PG_EXAMPLE,
        _ROUTING_PG_ES_EXAMPLE,
        _ROUTING_ES_ONLY_EXAMPLE,
        _ROUTING_DUCKDB_ES_EXAMPLE,
        _ROUTING_PG_ES_ICEBERG_EXAMPLE,
        _ROUTING_GEOPARQUET_EXAMPLE,
    ],
    WellKnownPlugin.STORAGE_ASSETS: [
        _ROUTING_ASSETS_PG_EXAMPLE,
        _ROUTING_ASSETS_ES_EXAMPLE,
    ],
    # Legacy aliases
    WellKnownPlugin.ROUTING: [
        _ROUTING_PG_EXAMPLE,
        _ROUTING_PG_ES_EXAMPLE,
        _ROUTING_ES_ONLY_EXAMPLE,
        _ROUTING_DUCKDB_ES_EXAMPLE,
        _ROUTING_PG_ES_ICEBERG_EXAMPLE,
        _ROUTING_GEOPARQUET_EXAMPLE,
    ],
    WellKnownPlugin.ROUTING_ASSETS: [
        _ROUTING_ASSETS_PG_EXAMPLE,
        _ROUTING_ASSETS_ES_EXAMPLE,
    ],
    WellKnownPlugin.DRIVER_POSTGRESQL: [
        _DRIVER_PG_MINIMAL_EXAMPLE,
        _DRIVER_PG_PARTITIONED_EXAMPLE,
    ],
    WellKnownPlugin.DRIVER_ELASTICSEARCH: [
        _DRIVER_ES_EXAMPLE,
        _DRIVER_ES_RASTER_EXAMPLE,
        _DRIVER_ES_CUSTOM_MAPPING_EXAMPLE,
    ],
    WellKnownPlugin.DRIVER_ELASTICSEARCH_ASSETS: [
        _DRIVER_ES_ASSETS_EXAMPLE,
    ],
    WellKnownPlugin.DRIVER_DUCKDB: [
        _DRIVER_DUCKDB_PARQUET_EXAMPLE,
        _DRIVER_DUCKDB_CSV_EXAMPLE,
        _DRIVER_DUCKDB_GEOPARQUET_EXAMPLE,
    ],
    WellKnownPlugin.ELASTICSEARCH: [
        _ES_CATALOG_EXAMPLE,
        _ES_CATALOG_OBFUSCATED_EXAMPLE,
    ],
    WellKnownPlugin.STAC: [_STAC_MINIMAL_EXAMPLE, _STAC_DATACUBE_EXAMPLE],
    WellKnownPlugin.COLLECTION: [_COLLECTION_EXAMPLE],
    WellKnownPlugin.TILES: [_TILES_EXAMPLE],
    WellKnownPlugin.FEATURES: [_FEATURES_EXAMPLE],
    WellKnownPlugin.TASKS: [_TASKS_EXAMPLE],
}


def get_plugin_examples(plugin_id: str) -> Optional[List[Dict[str, Any]]]:
    """Return OpenAPI examples for a known plugin, or ``None``."""
    return PLUGIN_EXAMPLES.get(plugin_id)


# ---------------------------------------------------------------------------
# Composite "Quick Start" payload
# ---------------------------------------------------------------------------


class QuickStartConfigSet(BaseModel):
    """A ready-to-apply bundle of configurations for bootstrapping a catalog
    or collection with PostgreSQL defaults.

    Each key is a ``plugin_id``; the value is the config payload.
    Use ``PUT /configs/{plugin_id}`` (or the catalog/collection variant)
    to apply each entry.
    """

    configs: Dict[str, Dict[str, Any]] = Field(
        ...,
        description="Map of plugin_id to config payload.",
        json_schema_extra={
            "examples": [
                {
                    "storage:collections": _ROUTING_PG_EXAMPLE["value"],
                    "storage:assets": _ROUTING_ASSETS_PG_EXAMPLE["value"],
                    "driver:postgresql": _DRIVER_PG_MINIMAL_EXAMPLE["value"],
                    "driver:postgresql_assets": {"enabled": True},
                    "stac": _STAC_MINIMAL_EXAMPLE["value"],
                    "collection": _COLLECTION_EXAMPLE["value"],
                    "tiles": _TILES_EXAMPLE["value"],
                    "features": _FEATURES_EXAMPLE["value"],
                    "tasks": _TASKS_EXAMPLE["value"],
                }
            ]
        },
    )


# ---------------------------------------------------------------------------
# Bulk-apply response
# ---------------------------------------------------------------------------


class BulkApplyResultEntry(BaseModel):
    """Result of applying a single plugin config within a bulk operation."""

    plugin_id: str
    status: str = Field(..., description="'ok' or 'error'.")
    detail: Optional[str] = Field(None, description="Error message if status is 'error'.")


class BulkApplyResponse(BaseModel):
    """Response from the bulk-apply endpoint."""

    applied: int = Field(0, description="Number of configs successfully applied.")
    failed: int = Field(0, description="Number of configs that failed.")
    results: List[BulkApplyResultEntry] = Field(default_factory=list)
