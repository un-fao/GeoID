# DynaStore Configurations Extension

The Configurations Extension provides a unified, hierarchical system for managing settings across the entire DynaStore application. It allows administrators to define configuration defaults at a global (platform) level, override them for specific catalogs, and further specialize them for individual collections.

This system is pluggable, meaning any other extension or module can register its own configuration model, making its settings automatically manageable through this central API. The extension is designed to be extensible, allowing any module or other extension to register its own settings and make them centrally manageable.

## Core Architecture

The configuration system is built on two key modules: `db_config` and `catalog`.

### The Foundation: `db_config` Module

The `db_config` module provides the fundamental building blocks for the entire system:

*   **`PluginConfig`**: A Pydantic `BaseModel` that all configuration models must inherit from.
*   **`ConfigRegistry`**: A central, in-memory registry that maps a unique string ID (the `plugin_id`) to its corresponding `PluginConfig` model. This allows the system to validate and instantiate configuration objects dynamically.
*   **`@register_config(plugin_id)`**: A decorator that automatically registers a `PluginConfig` model in the `ConfigRegistry`.
*   **`PlatformConfigManager`**: A manager responsible for storing and retrieving global, platform-wide configuration defaults in the `platform_configs` database table.
*   **`Immutable` Annotation**: A marker used with `typing.Annotated` to flag certain configuration fields as immutable. The framework will prevent changes to these fields once a configuration has been saved, which is critical for settings that define physical infrastructure (e.g., a table's partitioning scheme).

### The Hierarchy: `catalog` Module Extension

The `catalog` module builds upon this foundation by introducing a hierarchical lookup mechanism. It defines its own `ConfigManager` that orchestrates configuration resolution in the following order of precedence:

1.  **Collection Level**: Settings specific to a single collection (e.g., `my_catalog:my_collection`), stored in `collection_configs`.
2.  **Catalog Level**: Settings that apply to all collections within a catalog (e.g., `my_catalog`), stored in `catalog_configs`.
3.  **Platform Level**: Global default settings managed by `PlatformConfigManager`.
4.  **Code Default**: The default values defined in the Pydantic model for the plugin's configuration.

When a configuration is requested for a specific collection, the `ConfigManager` traverses this hierarchy, merging settings to produce the final, effective configuration object.

## Use Cases

### Extending the System with a Custom Configuration

Any extension can add its own manageable settings by defining a Pydantic model and registering it.

For example, imagine a new "caching" extension that needs to control TTL values. It would define a model like this:

```python
# in my_caching_extension/caching_config.py
from pydantic import Field
from dynastore.modules.db_config.platform_config_manager import PluginConfig, register_config

# Define a unique ID for this configuration plugin
CACHING_PLUGIN_ID = "caching"

@register_config(CACHING_PLUGIN_ID)
class CachingConfig(PluginConfig):
    """Configuration for the Caching Extension."""
    ttl_seconds: int = Field(default=3600, description="Default cache Time-To-Live in seconds.")
    max_size_mb: int = Field(default=1024, description="Maximum cache size in megabytes.")

# Now, other parts of the application can get this configuration:
# from dynastore.modules.catalog import catalog_module
#
# async def some_caching_logic(catalog_id: str, collection_id: str):
#     config_service = catalog_module.get_config_service()
#     # This will resolve the config through the Collection > Catalog > Platform hierarchy
#     caching_config = await config_service.get_config(CACHING_PLUGIN_ID, catalog_id, collection_id)
#     # caching_config will be an instance of CachingConfig
#     print(f"Using cache TTL: {caching_config.ttl_seconds}")
```

### Discovering Available Configurations via API

The `configs` extension provides a discovery endpoint to see all registered plugins. This is invaluable for administration and for building dynamic user interfaces.

**1. List all available `plugin_id`s:**

```bash
curl http://localhost:8000/configs/plugins
```
**Response:**
```json
[
  "catalog",
  "stac",
  "gcp-catalog-bucket",
  "gcp-collection-bucket",
  "gcp-eventing",
  "caching"
]
```

**2. Get detailed schemas for all plugins:**

By adding `?with_schema=true`, you can retrieve the full JSON Schema for each plugin's model, including descriptions.

```bash
curl http://localhost:8000/configs/plugins?with_schema=true
```
**Response (snippet):**
```json
{
  "caching": {
    "description": "Configuration for the Caching Extension.",
    "schema": {
      "title": "CachingConfig",
      "description": "Configuration for the Caching Extension.",
      "type": "object",
      "properties": {
        "enabled": {
          "title": "Enabled",
          "default": true,
          "type": "boolean"
        },
        "ttl_seconds": {
          "title": "Ttl Seconds",
          "description": "Default cache Time-To-Live in seconds.",
          "default": 3600,
          "type": "integer"
        }
      }
    }
  }
}
```

## Quick Start — PostgreSQL Defaults

The most common setup is a catalog with PostgreSQL for all storage.
Use the **bulk-apply** endpoint to configure everything in a single call:

### 1. Configure a new catalog

```bash
curl -X PUT http://localhost:8000/configs/catalogs/my_catalog/bulk \
  -H "Content-Type: application/json" \
  -d '{
  "configs": {
    "routing": {
      "enabled": true,
      "operations": {
        "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
        "READ":  [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}]
      }
    },
    "routing_assets": {
      "enabled": true,
      "operations": {
        "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
        "READ":  [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}]
      }
    },
    "stac": {
      "enabled": true,
      "enabled_extensions": [],
      "asset_tracking": {"enabled": true, "access_mode": "DIRECT"}
    },
    "tiles":    {"enabled": true, "min_zoom": 0, "max_zoom": 12},
    "features": {"enabled": true},
    "tasks":    {"enabled": true, "queue_poll_interval": 30.0}
  }
}'
```

### 2. Configure a new collection with PG driver

```bash
curl -X PUT http://localhost:8000/configs/catalogs/my_catalog/collections/my_collection/bulk \
  -H "Content-Type: application/json" \
  -d '{
  "configs": {
    "driver:postgresql": {
      "enabled": true,
      "collection_type": "VECTOR",
      "sidecars": [
        {
          "sidecar_type": "geometries",
          "enabled": true,
          "target_srid": 4326,
          "target_dimension": "force_2d",
          "geom_column": "geom",
          "bbox_column": "bbox_geom",
          "invalid_geom_policy": "attempt_fix",
          "srid_mismatch_policy": "transform"
        },
        {
          "sidecar_type": "attributes",
          "enabled": true,
          "storage_mode": "automatic",
          "enable_external_id": true,
          "enable_asset_id": true,
          "versioning_behavior": "UPDATE_EXISTING_VERSION"
        }
      ],
      "partitioning": {"enabled": false, "partition_keys": []}
    },
    "routing": {
      "enabled": true,
      "operations": {
        "WRITE": [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}],
        "READ":  [{"driver_id": "postgresql", "hints": [], "on_failure": "fatal"}]
      }
    },
    "stac": {
      "enabled": true,
      "enabled_extensions": [],
      "asset_tracking": {"enabled": true, "access_mode": "DIRECT"}
    }
  }
}'
```

### 3. Browse available examples

```bash
# All plugins
curl http://localhost:8000/configs/examples

# Specific plugin
curl http://localhost:8000/configs/examples/driver:postgresql
```

## API Endpoints

This extension provides a RESTful API for managing configurations at all three levels of the hierarchy (Platform, Catalog, and Collection). It includes endpoints for:

*   **CRUD** — Listing, getting, setting, and deleting configurations at each level.
*   **Examples** — `GET /configs/examples` and `GET /configs/examples/{plugin_id}` return ready-to-use payloads.
*   **Bulk apply** — `PUT /configs/catalogs/{id}/bulk` and `PUT /configs/catalogs/{id}/collections/{id}/bulk` apply multiple plugins in one call.
*   **Search** — `GET /configs/catalogs/{id}/search?q=driver` filters configs by plugin name.
*   **Schema discovery** — `GET /configs/plugins?with_schema=true` returns JSON Schemas for all registered plugins.

For the full interactive documentation, see `/docs`.