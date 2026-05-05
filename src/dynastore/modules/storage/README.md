# Storage Module — Multi-Driver Routing

Entity-level storage abstraction that routes collection data to pluggable backends.

## Role

The storage module sits between the REST API layer and the storage backends. It resolves
which driver to use for a given catalog/collection based on `StorageRoutingConfig`, then
delegates all CRUD and lifecycle operations to that driver.

## Components

| File | Purpose |
|------|---------|
| `config.py` | `StorageRoutingConfig` — which driver(s) to use (PluginConfig, 4-tier hierarchy) |
| `router.py` | `get_driver()` — cached resolution of config → driver instance |
| `protocol.py` | Re-export of `StorageDriverProtocol` for convenience |
| `drivers/postgresql.py` | PostgreSQL driver — wraps existing `ItemsProtocol` services |
| `drivers/elasticsearch.py` | Elasticsearch drivers — SFEOS-backed full STAC + private |

## Public API

```python
from dynastore.modules.storage import get_driver, StorageRoutingConfig

# Read — hint selects the right backend
driver = await get_driver(catalog_id, collection_id, hint="search")
async for feature in driver.read_entities(catalog_id, collection_id, request=query):
    process(feature)

# Write — always goes to primary driver
driver = await get_driver(catalog_id, collection_id, write=True)
written = await driver.write_entities(catalog_id, collection_id, feature_collection)
```

## Configuration

Set via `ConfigsProtocol` at platform, catalog, or collection level.  Post-PR-#261 routing is **operation-based**: `operations: Dict[Operation, List[OperationDriverEntry]]` where each `Operation` (WRITE/READ/SEARCH/INDEX/BACKUP) maps to an ordered list of drivers with per-entry `on_failure` / `write_mode` / `hints`:

```json
{"operations": {
  "WRITE": [
    {"driver_id": "items_postgresql_driver", "on_failure": "fatal"},
    {"driver_id": "items_elasticsearch_driver", "write_mode": "async", "on_failure": "outbox"}
  ],
  "READ": [
    {"driver_id": "items_elasticsearch_driver", "hints": ["geometry_simplified"]},
    {"driver_id": "items_postgresql_driver", "hints": ["geometry_exact"]}
  ]
}}
```

The pre-PR-#261 `primary_driver` / `read_drivers` / `secondary_drivers` shape was retired with the operation-based migration; full reference in `docs/components/storage_drivers.md` (rewrite pending — see project memory).

## Drivers

### PostgreSQL (`postgresql`)

Wraps `ItemCrudProtocol` and `ItemQueryProtocol`. Zero SQL rewrite — all sidecar logic,
query optimization, PostGIS, and streaming stay in the existing service layer.

### Elasticsearch (`elasticsearch`)

Delegates to SFEOS `DatabaseLogic` for full STAC compatibility. Supports items, collections,
and catalogs. Registers as async event listener for secondary write fan-out.

### Elasticsearch Private (`elasticsearch_private`)

Stores `{geoid, catalog_id, collection_id}` with `dynamic: false` mapping. Only allows
search by geoid. Self-manages DENY access policies for catalog-level access control.

## Adding a Driver

1. Create `drivers/<name>.py`, subclass `ModuleProtocol`, set `driver_id`
2. Implement `StorageDriverProtocol` methods
3. Add entry point in `pyproject.toml`
4. Configure via `StorageRoutingConfig`

## Dependencies

- Core: `pydantic`, `cachetools` (always available)
- PostgreSQL driver: `dynastore[module_catalog]` (wraps existing services)
- Elasticsearch drivers: `stac-fastapi-elasticsearch` (optional, graceful degradation)
