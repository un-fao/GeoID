# Multi-Driver Storage Abstraction

Entity-level storage abstraction that routes collection data to pluggable backends.
Each driver is a self-contained module with its own connection lifecycle, location config,
and capabilities. Drivers are discovered via entry points and selected at runtime through
a hint-based routing system backed by a 4-tier config hierarchy.

## Architecture

```
REST API (hint="search"|"features"|"analytics"|...)
    │
    ▼
get_driver(catalog_id, collection_id, hint=..., write=...)
    │
    ├── StorageRoutingConfig (4-tier ConfigsProtocol hierarchy)
    │     └── primary_driver, read_drivers, secondary_drivers, storage_locations
    │
    ▼
CollectionStorageDriverProtocol implementation
    ├── PostgresStorageDriver         priority=10  (wraps ItemsProtocol)
    ├── IcebergStorageDriver          priority=20  (PyIceberg + PG SqlCatalog)
    ├── DuckDBStorageDriver           priority=30  (file-based analytical reads)
    ├── ElasticsearchStorageDriver    priority=50  (SFEOS DatabaseLogic)
    └── ElasticsearchPrivateDriver priority=55  (geoid-only, DENY policies)
```

### Key Design Decisions

- **Streaming-first**: `read_entities` returns `AsyncIterator[Feature]` — O(1) memory regardless of result size.
- **Entity-level abstraction**: All drivers work with typed Pydantic `Feature`/`FeatureCollection` models, not raw SQL or engine-specific queries.
- **Capability declaration**: Drivers declare what they support via `Capability` enum. The router can check capabilities before dispatching.
- **Lazy initialization**: Connections (DuckDB singleton, Iceberg catalog) are created on first use, not at import time.
- **Event-driven fan-out**: Secondary drivers receive writes asynchronously via the event system — no synchronous coupling.

## Quick Start

```python
from dynastore.modules.storage import get_driver

# Read — operation + hint selects the right backend
driver = await get_driver("READ", catalog_id, collection_id, hint="search")
async for feature in driver.read_entities(catalog_id, collection_id, request=query):
    process(feature)

# Write — operation-based routing to all configured write drivers
driver = await get_driver("WRITE", catalog_id, collection_id)
written = await driver.write_entities(catalog_id, collection_id, feature_collection)

# Check capabilities before calling OTF-specific methods
from dynastore.models.protocols.storage_driver import Capability
if Capability.TIME_TRAVEL in driver.capabilities:
    async for feature in driver.read_at_snapshot(catalog_id, collection_id, snapshot_id):
        ...
```

## CollectionStorageDriverProtocol

Defined in `dynastore.models.protocols.storage_driver`. All input/output types are Pydantic models.

### Core Methods

| Method | Input Types | Output Type | Description |
|--------|------------|-------------|-------------|
| `write_entities` | `Feature \| FeatureCollection \| Dict \| List[Dict]` | `List[Feature]` | Insert or upsert entities |
| `read_entities` | `QueryRequest`, entity_ids, limit, offset | `AsyncIterator[Feature]` | Stream entities with optional filters |
| `delete_entities` | `List[str]` entity IDs, `soft: bool` | `int` (count) | Hard or soft delete by ID |
| `ensure_storage` | catalog_id, collection_id | `None` | Create backing storage if it doesn't exist |
| `drop_storage` | catalog_id, collection_id, `soft: bool` | `None` | Remove or tag storage as deleted |
| `export_entities` | format, target_path | `str` (path) | Export collection to file |

### OTF Methods (Iceberg-specific, capability-gated)

| Method | Description | Required Capability |
|--------|-------------|---------------------|
| `list_snapshots` | List snapshot history | `SNAPSHOTS` |
| `create_snapshot` | Create named snapshot/branch | `SNAPSHOTS` |
| `rollback_to_snapshot` | Rollback to a previous snapshot | `SNAPSHOTS` |
| `read_at_snapshot` | Time-travel read at specific snapshot | `TIME_TRAVEL` |
| `read_at_timestamp` | Time-travel read at specific datetime | `TIME_TRAVEL` |
| `evolve_schema` | Add/rename/drop/promote columns | `SCHEMA_EVOLUTION` |
| `get_schema_history` | Schema version history | `SCHEMA_EVOLUTION` |

### Capabilities

```python
from dynastore.models.protocols.storage_driver import Capability

class Capability(StrEnum):
    READ_ONLY       = "read_only"
    STREAMING       = "streaming"
    SPATIAL_FILTER  = "spatial_filter"
    FULLTEXT        = "fulltext"
    EXPORT          = "export"
    TIME_TRAVEL     = "time_travel"
    SOFT_DELETE     = "soft_delete"
    VERSIONING      = "versioning"
    SCHEMA_EVOLUTION = "schema_evolution"
    SNAPSHOTS       = "snapshots"
```

## StorageRoutingConfig

Lives in the 4-tier `ConfigsProtocol` hierarchy (collection > catalog > platform > defaults).

```json
// Default — backward compatible, PG only
{"primary_driver": "postgresql"}

// PG primary + ES secondary (async fan-out via events)
{"primary_driver": "postgresql",
 "secondary_drivers": ["elasticsearch"]}

// Search routed to ES, everything else to PG
{"primary_driver": "postgresql",
 "read_drivers": {"search": "elasticsearch"},
 "secondary_drivers": ["elasticsearch"]}

// Analytical workload with Iceberg primary + DuckDB reads
{"primary_driver": "iceberg",
 "read_drivers": {"analytics": "duckdb", "default": "duckdb"},
 "storage_locations": {
   "iceberg": {
     "driver": "iceberg",
     "namespace": "analytics",
     "catalog_type": "sql"
   },
   "duckdb": {
     "driver": "duckdb",
     "path": "/data/exports/*.parquet",
     "format": "parquet"
   }
 }}

// Iceberg with GCS warehouse (auto-resolved from StorageProtocol)
{"primary_driver": "iceberg",
 "storage_locations": {
   "iceberg": {
     "driver": "iceberg",
     "catalog_type": "sql"
   }
 }}

// Iceberg with explicit S3 warehouse override
{"primary_driver": "iceberg",
 "storage_locations": {
   "iceberg": {
     "driver": "iceberg",
     "catalog_type": "sql",
     "warehouse_uri": "s3://my-bucket/iceberg/"
   }
 }}
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `primary_driver` | `DriverRef` | `"postgresql"` | Driver for writes and default reads |
| `read_drivers` | `Dict[str, DriverRef]` | `{}` | Hint-to-driver mapping for reads |
| `secondary_drivers` | `List[DriverRef]` | `[]` | Async write fan-out via events |
| *(driver configs)* | See driver-specific config classes in `driver_config.py` | — | Stored per-driver via `plugin_id="driver:<name>"` |

### Read Hints

The REST API passes a hint when requesting a driver for reads:

| Hint | Typical Use |
|------|-------------|
| `"default"` | General-purpose reads |
| `"search"` | STAC/fulltext search |
| `"features"` | OGC Features endpoint |
| `"graph"` | Graph traversal |
| `"analytics"` | DWH/analytical queries |
| `"cache"` | Cache-first hot reads |

If the hint is not in `read_drivers`, falls back to `"default"` entry, then to `primary_driver`.

Custom hints can be registered:
```python
from dynastore.modules.storage import register_hint
register_hint("tiles", "Tile-optimized reads for map rendering")
```

## Router Performance

Driver resolution is cached at 300s TTL (aligned with config cache) via `@cached(maxsize=4096, ttl=300)`.
The driver index (`driver_id -> instance`) is rebuilt from the discovery registry on each lookup.

---

## Driver Reference

### PostgreSQL Driver

| Property | Value |
|----------|-------|
| **driver_id** | `postgresql` |
| **priority** | 10 |
| **capabilities** | `STREAMING`, `SPATIAL_FILTER`, `SOFT_DELETE`, `EXPORT` |
| **driver config** | `PostgresCollectionDriverConfig` (`plugin_id="driver:postgresql"`) |
| **dependencies** | Core (always available) |

Wraps existing `ItemCrudProtocol` and `ItemQueryProtocol`. Zero SQL rewrite — all sidecar logic,
query optimization, PostGIS, and streaming stay in the existing service layer.

```
write_entities  → ItemCrudProtocol.upsert()
read_entities   → ItemQueryProtocol.stream_items()  (O(1) streaming)
delete_entities → ItemCrudProtocol.delete_item()
ensure_storage  → CatalogsProtocol lifecycle
drop_storage    → CatalogsProtocol.delete_collection/catalog()
```

**Location config:**
```json
{
  "driver": "postgresql",
  "physical_schema": "public",
  "physical_table": "my_custom_table"
}
```

### Iceberg Driver

| Property | Value |
|----------|-------|
| **driver_id** | `iceberg` |
| **priority** | 20 |
| **capabilities** | `STREAMING`, `SPATIAL_FILTER`, `EXPORT`, `TIME_TRAVEL`, `VERSIONING`, `SNAPSHOTS`, `SCHEMA_EVOLUTION`, `SOFT_DELETE` |
| **driver config** | `IcebergCollectionDriverConfig` (`plugin_id="driver:iceberg"`) |
| **dependencies** | `pyiceberg[sql-postgres]>=0.9.0`, `pyarrow>=14.0.0` |

Full Open Table Format support via PyIceberg: ACID transactions, snapshots, time-travel reads,
and schema evolution — all backed by a real Iceberg catalog.

**Catalog strategy:**
- **Default**: PostgreSQL-backed `SqlCatalog` using the platform's `DATABASE_URL` (zero config)
- Uses `pyiceberg.catalog.CatalogType` enum and PyIceberg constants (`TYPE`, `URI`, `WAREHOUSE_LOCATION`) — no string literals
- PyIceberg creates lightweight metadata tables (`iceberg_tables`, `iceberg_namespace_properties`) — no conflict with DynaStore schema
- Other catalog types supported via `CatalogType`: `REST`, `GLUE`, `HIVE`, `DYNAMODB`, `BIGQUERY`, `IN_MEMORY`

**Warehouse auto-resolution** (via `_resolve_warehouse()` → `_ensure_catalog()`):

1. **Explicit** — `warehouse_uri` field in `IcebergCollectionDriverConfig` (manual override)
2. **Auto-detected** — from platform `StorageProtocol` (e.g., GCS bucket). When a collection already has a GCP bucket, the driver derives `gs://bucket/.../iceberg/` automatically
3. **Fallback** — local temp dir (`file:///tmp/iceberg_warehouse`)

When the warehouse URI starts with `gs://`, the driver injects `GCS_PROJECT_ID` from environment for PyArrowFileIO. The same pattern applies for future `s3://` support.

```
write_entities       → PyArrow append to Iceberg table
read_entities        → PyIceberg scan with expression filters
delete_entities      → Positional deletes (soft) or delete+compact (hard)
ensure_storage       → Create namespace + table with schema
drop_storage         → Drop table or tag with dynastore.deleted property
export_entities      → Scan → write to parquet/csv/json
list_snapshots       → table.history() → SnapshotInfo list
read_at_snapshot     → scan(snapshot_id=...) → time-travel read
read_at_timestamp    → Find snapshot at timestamp → delegate to read_at_snapshot
evolve_schema        → table.update_schema() with add/rename/drop/type-promote
```

**DynaStore hierarchy mapping:**
- DynaStore catalog → Iceberg namespace
- DynaStore collection → Iceberg table
- DynaStore entity → Iceberg row

**Location config (minimal — warehouse auto-resolved from GCS bucket):**
```json
{
  "driver": "iceberg",
  "catalog_type": "sql",
  "namespace": "analytics"
}
```

**Location config (explicit warehouse override):**
```json
{
  "driver": "iceberg",
  "catalog_name": "production",
  "catalog_type": "sql",
  "catalog_uri": "postgresql+psycopg2://user:pass@host:5432/db",
  "warehouse_uri": "gs://my-bucket/iceberg/",
  "namespace": "analytics",
  "table_name": "observations"
}
```

When `catalog_uri` is omitted and `catalog_type` is `"sql"`, the driver auto-resolves from `DBConfig.database_url`.

**Production deployment:**
```
PostgreSQL ─── Iceberg SQL Catalog (metadata: table locations, snapshots, schemas)
    │
    └── Warehouse Storage (data files: Parquet)
         ├── Local filesystem (dev/test)
         ├── S3 (AWS production)
         └── GCS (GCP production)
```

**Install:**
```bash
pip install dynastore[module_storage_iceberg]
```

### DuckDB Driver

| Property | Value |
|----------|-------|
| **driver_id** | `duckdb` |
| **priority** | 30 |
| **capabilities** | `READ_ONLY`, `STREAMING`, `SPATIAL_FILTER`, `EXPORT` |
| **driver config** | `DuckDbCollectionDriverConfig` (`plugin_id="driver:duckdb"`) |
| **dependencies** | `duckdb>=1.0.0` |

File-based analytical reads via DuckDB's built-in readers. Reads from parquet, CSV, JSON, etc.
Optionally writes to SQLite when `write_path` is configured.

**Connection model:** Process-wide singleton DuckDB connection (in-memory, thread-safe).
Extensions (`spatial`, `sqlite`) are loaded once at connection creation.

```
write_entities  → SQLite writes via DuckDB's sqlite extension (if write_path set)
read_entities   → DuckDB reader functions (read_parquet, read_csv_auto, etc.)
delete_entities → DELETE from SQLite (hard only, no soft delete)
export_entities → COPY ... TO (format: parquet, csv, json)
```

**Location config:**
```json
{
  "driver": "duckdb",
  "path": "/data/observations/*.parquet",
  "format": "parquet",
  "write_path": "/data/cache.sqlite",
  "write_format": "sqlite"
}
```

**Install:**
```bash
pip install dynastore[module_storage_duckdb]
```

### Elasticsearch Driver

| Property | Value |
|----------|-------|
| **driver_id** | `elasticsearch` |
| **priority** | 50 |
| **capabilities** | `STREAMING`, `SPATIAL_FILTER`, `FULLTEXT`, `SOFT_DELETE` |
| **driver config** | `ElasticsearchCollectionDriverConfig` (`plugin_id="driver:elasticsearch"`) |
| **dependencies** | `stac-fastapi-elasticsearch` (optional) |

Delegates all ES operations to SFEOS `DatabaseLogic`, ensuring full read/write compatibility
with `stac-fastapi-elasticsearch-opensearch`. Supports items, collections, and catalogs.

```
write_entities  → DatabaseLogic.create_item() / bulk_async()
read_entities   → DatabaseLogic.get_one_item() / client.search()
delete_entities → DatabaseLogic.delete_item()
ensure_storage  → create_index_templates() + create_collection_index()
drop_storage    → DatabaseLogic.delete_collection() / delete_catalog()
```

Additional methods: `write_catalog()`, `delete_catalog()`, `write_collection()`, `delete_collection_doc()`.

**Event handlers:** Registers as async event listener for catalog, collection, and item lifecycle.
Only acts when listed in `StorageRoutingConfig.secondary_drivers`.

### Elasticsearch Private Driver

| Property | Value |
|----------|-------|
| **driver_id** | `elasticsearch_private` |
| **priority** | 55 |
| **capabilities** | `STREAMING` |
| **dependencies** | `stac-fastapi-elasticsearch` (optional) |

Stores `{geoid, catalog_id, collection_id}` in a custom index with `dynamic: false` mapping.
No geometry, no attributes, no spatial search — geoid lookup only.

```
write_entities  → bulk index to private index
read_entities   → es.get() by geoid → Feature with null geometry
ensure_storage  → create private index + apply DENY policy
drop_storage    → delete index + revoke DENY policy
```

**DENY policy management:** On `ensure_storage`, applies a DENY policy blocking public access.
On `drop_storage`, revokes it. On startup (`lifespan`), restores DENY policies.

---

## Driver Config System

Each driver has its own typed config class in `driver_config.py` (subclass of `CollectionDriverConfig`).
Fetch via the config waterfall using the driver's `_plugin_id`:

```python
from dynastore.modules.storage.driver_config import (
    PostgresCollectionDriverConfig,
    DuckDbCollectionDriverConfig,
    IcebergCollectionDriverConfig,
)

# PG config (convenience helper):
from dynastore.modules.storage.driver_config import get_pg_collection_config
config = await get_pg_collection_config(catalog_id, collection_id)

# Other drivers — use the config service directly:
from dynastore.tools.discovery import get_protocol
from dynastore.models.protocols import ConfigsProtocol
configs = get_protocol(ConfigsProtocol)
config = await configs.get_config(
    IcebergCollectionDriverConfig._plugin_id,
    catalog_id=catalog_id,
    collection_id=collection_id,
)
```

### Driver Config Types

| Driver | Config Class | Plugin ID | Key Fields |
|--------|-------------|-----------|------------|
| `postgresql` | `PostgresCollectionDriverConfig` | `driver:postgresql` | `physical_schema`, `physical_table`, `sidecars`, `partitioning` |
| `duckdb` | `DuckDbCollectionDriverConfig` | `driver:duckdb` | `path`, `format`, `write_path`, `write_format` |
| `iceberg` | `IcebergCollectionDriverConfig` | `driver:iceberg` | `catalog_name`, `catalog_type`, `catalog_uri`, `catalog_properties`, `warehouse_uri`, `warehouse_scheme`, `namespace`, `table_name` |

---

## Creating a New Driver

### Step 1: Create the Driver Module

Create `src/dynastore/modules/storage/drivers/<name>.py`:

```python
"""
MyDatabase Storage Driver — brief description of what it does.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, FrozenSet, List, Optional, Union

from dynastore.models.ogc import Feature, FeatureCollection
from dynastore.models.protocols.storage_driver import Capability
from dynastore.models.query_builder import QueryRequest
from dynastore.modules.protocols import ModuleProtocol

logger = logging.getLogger(__name__)


class MyDatabaseStorageDriver(ModuleProtocol):
    """MyDatabase storage driver — what it does and when to use it."""

    # --- Required class attributes ---
    driver_id: str = "mydatabase"       # Unique ID used in routing config
    priority: int = 40                   # Lower = higher priority

    # Declare what this driver supports
    capabilities: FrozenSet[str] = frozenset({
        Capability.STREAMING,
        Capability.EXPORT,
    })

    def is_available(self) -> bool:
        """Return True if the driver's dependencies are installed."""
        try:
            import mydatabase_client  # noqa: F401
            return True
        except ImportError:
            return False

    @asynccontextmanager
    async def lifespan(self, app_state: object):
        """Startup/shutdown lifecycle hook."""
        logger.info("MyDatabaseStorageDriver: started")
        # Initialize connections, pools, etc.
        yield
        # Cleanup connections
        logger.info("MyDatabaseStorageDriver: stopped")

    # --- Core CRUD ---

    async def write_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entities: Union[Feature, FeatureCollection, Dict[str, Any], List[Dict[str, Any]]],
        *,
        db_resource: Optional[Any] = None,
    ) -> List[Feature]:
        # 1. Normalize input to flat dicts
        from dynastore.modules.storage.drivers._duckdb_helpers import (
            normalize_to_dicts, dicts_to_features,
        )
        rows = normalize_to_dicts(entities)
        if not rows:
            return []

        # 2. Write to your backend
        # ...

        # 3. Return Feature models
        return dicts_to_features(rows)

    async def read_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        entity_ids: Optional[List[str]] = None,
        request: Optional[QueryRequest] = None,
        limit: int = 100,
        offset: int = 0,
        db_resource: Optional[Any] = None,
    ) -> AsyncIterator[Feature]:
        # Stream results — yield Feature objects one at a time
        # for row in backend.query(...):
        #     yield Feature(type="Feature", id=row["id"], ...)
        return
        yield  # Make this an async generator

    async def delete_entities(
        self,
        catalog_id: str,
        collection_id: str,
        entity_ids: List[str],
        *,
        soft: bool = False,
        db_resource: Optional[Any] = None,
    ) -> int:
        if soft:
            from dynastore.modules.storage.errors import SoftDeleteNotSupportedError
            raise SoftDeleteNotSupportedError(
                "MyDatabaseStorageDriver does not support soft delete."
            )
        # Delete rows and return count
        return len(entity_ids)

    # --- Storage lifecycle ---

    async def ensure_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
    ) -> None:
        """Create tables/indices/namespaces if they don't exist."""
        pass

    async def drop_storage(
        self,
        catalog_id: str,
        collection_id: Optional[str] = None,
        *,
        soft: bool = False,
    ) -> None:
        """Remove or tag storage as deleted."""
        pass

    async def export_entities(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        format: str = "parquet",
        target_path: str = "",
        db_resource: Optional[Any] = None,
    ) -> str:
        """Export collection data to a file."""
        raise NotImplementedError("Export not supported")
```

### Step 2: Create a Driver Config (Optional)

If your driver needs custom configuration, add a subclass of `CollectionDriverConfig` in `driver_config.py`:

```python
# In src/dynastore/modules/storage/driver_config.py

class MyDatabaseCollectionDriverConfig(CollectionDriverConfig):
    """Config for MyDatabase driver."""

    _plugin_id: ClassVar[Optional[str]] = "driver:mydatabase"
    connection_pool_size: int = Field(10, description="Connection pool size")
    read_preference: str = Field("primary", description="Read preference")
```

Store and retrieve via the config service using `_plugin_id` as the key.

### Step 3: Register the Entry Point

Add to `pyproject.toml`:

```toml
[project.entry-points."dynastore.modules"]
storage_mydatabase = "dynastore.modules.storage.drivers.mydatabase:MyDatabaseStorageDriver"
```

### Step 4: Add Optional Dependencies

If your driver has external dependencies, add an optional dependency group:

```toml
[project.optional-dependencies]
module_storage_mydatabase = ["mydatabase-client>=1.0.0"]
```

### Step 5: Configure Routing

Use the driver in any tier of `StorageRoutingConfig`:

```json
{
  "primary_driver": "postgresql",
  "read_drivers": {
    "analytics": "mydatabase"
  },
  "storage_locations": {
    "mydatabase": {
      "driver": "mydatabase",
      "uri": "mydatabase://host:port/db",
      "connection_pool_size": 20
    }
  }
}
```

### Step 6: Write Tests

Create `tests/dynastore/modules/storage/unit/test_mydatabase_driver.py`. Use real connections
where possible (following the Iceberg test pattern with `SqlCatalog` backed by PG):

```python
import pytest
from dynastore.modules.storage.drivers.mydatabase import MyDatabaseStorageDriver


class TestMyDatabaseDriverMeta:
    def test_driver_id(self):
        assert MyDatabaseStorageDriver().driver_id == "mydatabase"

    def test_capabilities(self):
        caps = MyDatabaseStorageDriver().capabilities
        assert "streaming" in caps


class TestMyDatabaseWriteEntities:
    @pytest.mark.asyncio
    async def test_write_and_read_back(self, driver, test_loc):
        # Write → read → verify
        ...
```

### Checklist

- [ ] `driver_id` is unique and matches config strings
- [ ] `is_available()` returns `False` when dependencies are missing (graceful degradation)
- [ ] `capabilities` accurately declares supported features
- [ ] `lifespan()` properly initializes and cleans up resources
- [ ] `read_entities` is an async generator (streaming, not buffered)
- [ ] `write_entities` handles all input types: `Feature`, `FeatureCollection`, `Dict`, `List[Dict]`
- [ ] `delete_entities` raises `SoftDeleteNotSupportedError` if `soft=True` and unsupported
- [ ] Entry point registered in `pyproject.toml`
- [ ] Tests use real connections where feasible (no MagicMock for the backend)

---

## Utility Functions

Shared helpers in `dynastore.modules.storage.drivers._duckdb_helpers`:

```python
from dynastore.modules.storage.drivers._duckdb_helpers import (
    normalize_to_dicts,   # Feature/FeatureCollection/Dict → List[Dict]
    dicts_to_features,    # List[Dict] → List[Feature] (extracts id, geometry, properties)
)
```

These are backend-agnostic and used by DuckDB, Iceberg, and any driver that works with tabular data.

## Error Types

```python
from dynastore.modules.storage.errors import (
    ReadOnlyDriverError,          # Write attempted on a read-only driver
    SoftDeleteNotSupportedError,  # soft=True on a driver without SOFT_DELETE capability
)
```

`ReadOnlyDriverMixin` is available for drivers that only support reads — it raises
`ReadOnlyDriverError` on all write/delete/drop calls automatically.

## File Layout

```
src/dynastore/
├── models/
│   ├── protocols/
│   │   └── storage_driver.py            # Protocol + Capability enum
│   ├── ogc.py                           # Feature, FeatureCollection
│   ├── otf.py                           # SnapshotInfo, SchemaVersion, SchemaEvolution
│   └── query_builder.py                 # QueryRequest
├── modules/storage/
│   ├── __init__.py                      # Public API exports
│   ├── protocol.py                      # Re-export convenience
│   ├── config.py                        # StorageRoutingConfig (PluginConfig)
│   ├── driver_config.py                 # CollectionDriverConfig hierarchy (PG, DuckDB, Iceberg, ES)
│   ├── router.py                        # get_driver() with cached resolution
│   ├── errors.py                        # ReadOnlyDriverError, SoftDeleteNotSupportedError
│   └── drivers/
│       ├── __init__.py
│       ├── _duckdb_helpers.py           # normalize_to_dicts, dicts_to_features
│       ├── postgresql.py                # PostgresStorageDriver
│       ├── iceberg.py                   # IcebergStorageDriver
│       ├── duckdb.py                    # DuckDBStorageDriver
│       └── elasticsearch.py             # ES + ES Private drivers
└── docs/components/
    └── storage_drivers.md               # This file
```
