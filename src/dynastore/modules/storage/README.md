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
| `drivers/elasticsearch.py` | Elasticsearch drivers — SFEOS-backed full STAC + obfuscated |

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

Set via `ConfigsProtocol` at platform, catalog, or collection level:

```json
{"primary_driver": "postgresql"}

{"primary_driver": "postgresql",
 "read_drivers": {"search": "elasticsearch"},
 "secondary_drivers": ["elasticsearch"]}

{"primary_driver": "postgresql",
 "secondary_drivers": ["elasticsearch", "elasticsearch_obfuscated"]}
```

## Drivers

### PostgreSQL (`postgresql`)

Wraps `ItemCrudProtocol` and `ItemQueryProtocol`. Zero SQL rewrite — all sidecar logic,
query optimization, PostGIS, and streaming stay in the existing service layer.

### Elasticsearch (`elasticsearch`)

Delegates to SFEOS `DatabaseLogic` for full STAC compatibility. Supports items, collections,
and catalogs. Registers as async event listener for secondary write fan-out.

### Elasticsearch Obfuscated (`elasticsearch_obfuscated`)

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
