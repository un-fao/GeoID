# GeoID

GeoID Geospatial Framework

GeoID is a high-performance, scalable, and modular geospatial data serving framework built with FastAPI. It provides a unified backend for storing, managing, and serving geospatial data through a variety of OGC and community standards.

Big Picture: Architecture and Data Model
The core of GeoID's architecture is the clear and consistent mapping between its business logic (APIs and services) and the underlying PostgreSQL/PostGIS database storage. This design ensures that data is managed logically and that services can interact with it in a standardized way.

Storage-to-Business-Model Mapping
The framework's data hierarchy is designed to be intuitive and directly reflects the structure of the database:

Root Catalog (/): This is the top-level entry point for the entire system. It represents the PostgreSQL database instance itself, providing a list of all available Datasets.

Dataset (/{dataset} or /{store}): A Dataset is the primary organizational unit and maps directly to a database schema. It acts as a container for related geospatial data layers. This concept is consistently used across all OGC APIs (features, maps, tiles) to group collections.

Collection (/{dataset}/collections/{collectionId}): A Collection represents a specific geospatial data layer and maps directly to a database table within a schema. Each collection holds features of a similar type and schema.

Feature (/{dataset}/collections/{collectionId}/items/{featureId}): A Feature is the fundamental data unit, representing a single geospatial entity. It maps directly to a row in a database table.

This hierarchical model ensures a clean separation of concerns and allows different services to operate on the same underlying data structure in a consistent and predictable manner.

Implemented Extensions
GeoID is built as a collection of modular extensions, each providing a specific API standard.

Core Data Access & Management
OGC API - Features: The foundational service for CRUD (Create, Read, Update, Delete) operations on geospatial features. It serves as the primary interface for managing the data stored in the database.

STAC (SpatioTemporal Asset Catalog): Provides a standardized way to discover and catalog geospatial assets. It works in tandem with the Features API, offering a metadata-rich view of the same underlying data.

This modular architecture allows clients to interact with the same underlying data through the most appropriate standard for their needs, whether it's raw data access, metadata discovery, or visualization.

## Features

- **Plugin/Extension System:** Easily add new routes and functionalities via extensions.
- **STAC API:** Data discovery and cataloging using the SpatioTemporal Asset Catalog standard.
- **OGC API:** Data management and access using OGC API - Features.
- **Database Integrations:** Support for PostgreSQL/PostGIS, BigQuery, and more.
- **Async & Fast:** Built on FastAPI for high performance and async operations.

## Getting Started

### Prerequisites

- Python 3.12+
- Docker & Docker Compose (for containerized deployment)

### Installation (Development)
GeoID offers two main installation methods for development, depending on your needs.

#### Method 1: Complete Installation (Recommended for Testing)
This method installs the core framework, all tools, and dependencies for **all** available modules and extensions automatically. It is the simplest way to get a full development environment running.

1.  **Clone and activate the virtual environment:**
    ```bash
    git clone https://github.com/un-fao/GeoID.git
    cd GeoID
    python -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install all dependencies:**
    ```bash
    pip install -e '.[all]'
    ```

#### Method 2: Selective Installation
This method is for advanced use cases where you only want to install a specific subset of modules (e.g., for a lightweight production container).

1.  **Set environment variables** to specify which components to enable:
    ```bash
    export GeoID_MODULES="catalog,db"
    export GeoID_EXTENSION_MODULES="api,features"
    ```

2.  **Install the selected dependencies** along with testing tools:
    ```bash
    pip install -e '.[test, GeoID-enabled-modules]'
    ```

### Running with Docker Compose

GeoID uses a layered Docker Compose configuration to support different environments:
- **Production**: `docker compose up -d --build` (uses `docker-compose.yml`)
- **Development**: `./manage.sh dev` (merges `docker-compose.yml` and `docker-compose.dev.yml` with hot-reload and debugging)
The core services are:
- **api**: High-performance REST API serving geospatial data.
- **worker**: Background task processor (e.g., for ingestion and pre-seeding).
- **db**: PostgreSQL/PostGIS database with `pg_cron` support.

### Accessing the API

- API root: [http://localhost:8000](http://localhost:8000)
- Docs: [http://localhost:8000/docs](http://localhost:8000/docs)


## Configuration
Configuration is managed via environment variables and a unified **Hierarchical Configuration API**.

### Unified Configuration API
GeoID provides a central REST API for managing plugin configurations at three levels:
1.  **Platform**: Global defaults for all catalogs and collections.
2.  **Catalog**: Overrides for a specific database schema.
3.  **Collection**: Overrides for a specific table.

Discovery & Summary Endpoints:
- `GET /configs/plugins`: List all *registered* configuration schemas.
- `GET /configs/summary`: Get a high-level overview of which levels/catalogs/collections have active overrides (returns counts for scalability).
- `GET /configs/catalogs`: **Paginated** list of all `catalog_id`s with active overrides.
- `GET /configs/collections`: **Paginated** list of all collections with active overrides (filterable by `catalog_id`).
- `GET /configs/plugins/{plugin_id}/search`: Search for all explicit overrides of a specific plugin across the entire hierarchy.

Management Endpoints:
- `GET/PUT/DELETE /configs/plugins/{plugin_id}`: Manage platform-level config.
- `GET/PUT/DELETE /configs/catalogs/{catalog_id}/plugins/{plugin_id}`: Manage catalog-level config.
- `GET/PUT/DELETE /configs/catalogs/{catalog_id}/collections/{collection_id}/plugins/{plugin_id}`: Manage collection-level config.

### Immutability Framework
To ensure data integrity, GeoID enforces immutability for configuration fields that affect the physical storage structure (e.g., partitioning, geometry type, indexed columns).

**Usage in Pydantic Models:**
```python
from GeoID.modules.db_config.platform_config_manager import Immutable

class MyPluginConfig(PluginConfig):
    # This field cannot be changed once established in the DB
    geometry_type: Immutable[str] = Field("Point")
    
    # This field is mutable
    description: str = Field("My data")
```
Attempts to modify an `Immutable` field via the API will result in a `409 Conflict` error.

## Development


Catalog module features:

- Lifespan hooks can be added for startup/shutdown logic.

Sync/Async Event Listeners Guide
Overview
The Event System now supports dual-mode event listeners:

Sync Event Listeners - Run in-transaction (blocking)
Async Event Listeners - Run in background (non-blocking)
This matches the Lifecycle Framework's architecture and provides fine-grained control over event handling.

Usage
Sync Event Listeners (In-Transaction)
Use when you need transactional guarantees or must complete before the transaction commits.

from GeoID.modules.catalog.event_manager import (
    sync_event_listener,
    CatalogEventType
)
@sync_event_listener(CatalogEventType.COLLECTION_CREATION)
async def on_collection_created_sync(catalog_id: str, collection_id: str, **kwargs):
    """
    Runs IN-TRANSACTION when a collection is created.
    - Blocks the transaction until complete
    - Has access to db_resource from kwargs
    - Failures will rollback the transaction
    """
    db_resource = kwargs.get('db_resource')
    # Perform transactional operations
    await update_metadata(db_resource, catalog_id, collection_id)
Async Event Listeners (Background)
Use for slow operations that don't need transactional guarantees (external APIs, notifications, etc.).

from GeoID.modules.catalog.event_manager import (
    async_event_listener,
    CatalogEventType
)
@async_event_listener(CatalogEventType.COLLECTION_DELETION)
async def on_collection_deleted_async(catalog_id: str, collection_id: str, **kwargs):
    """
    Runs IN BACKGROUND when a collection is deleted.
    - Doesn't block the transaction
    - Runs via run_in_background
    - Failures are logged but don't affect the transaction
    """
    # Perform slow external operations
    await send_notification(f"Collection {collection_id} deleted")
    await update_external_index(catalog_id, collection_id)
Execution Flow
When 
emit()
 is called:
await emit_event(
    CatalogEventType.COLLECTION_CREATION,
    catalog_id="my_catalog",
    collection_id="my_collection",
    db_resource=conn
)
Execution order:

Event persisted to outbox (if db_resource provided)
Sync listeners execute sequentially (in-transaction, blocking)
Async listeners scheduled in background (non-blocking)
Function returns
When 
emit_detached()
 is called:
event_manager.emit_detached(
    CatalogEventType.COLLECTION_DELETION,
    catalog_id="my_catalog",
    collection_id="my_collection"
)
Execution order:

Only async listeners are scheduled
Sync listeners are NOT called
Function returns immediately
Migration from Old System
Before (Single Listener Type)
@register_event_listener(CatalogEventType.COLLECTION_CREATION)
async def on_collection_created(catalog_id, collection_id, **kwargs):
    # Unclear if this blocks or not
    # Unclear if transactional or not
    pass
After (Explicit Sync/Async)
# For transactional operations
@sync_event_listener(CatalogEventType.COLLECTION_CREATION)
async def on_collection_created_sync(catalog_id, collection_id, **kwargs):
    db_resource = kwargs.get('db_resource')
    # Guaranteed to run in-transaction
    pass
# For background operations
@async_event_listener(CatalogEventType.COLLECTION_CREATION)
async def on_collection_created_async(catalog_id, collection_id, **kwargs):
    # Guaranteed to run in background
    pass
Best Practices
Use Sync Listeners When:
Updating database records that must be atomic with the event
Validating data before commit
Enforcing business rules that affect transaction success
Need access to uncommitted data in the transaction
Use Async Listeners When:
Sending notifications (email, webhooks, etc.)
Updating external systems (search indexes, caches)
Performing slow operations (API calls, file I/O)
Operations that can tolerate eventual consistency
Example: GCP Module
# Sync: Update database tracking
@sync_event_listener(CatalogEventType.COLLECTION_CREATION)
async def track_gcp_collection_sync(catalog_id, collection_id, **kwargs):
    db_resource = kwargs.get('db_resource')
    await update_gcp_tracking_table(db_resource, catalog_id, collection_id)
# Async: Create GCP bucket (slow)
@async_event_listener(CatalogEventType.COLLECTION_CREATION)
async def create_gcp_bucket_async(catalog_id, collection_id, **kwargs):
    # Runs in background, doesn't block transaction
    bucket_name = f"{catalog_id}-{collection_id}"
    await create_gcs_bucket(bucket_name)
Comparison with Lifecycle Hooks
Feature	Lifecycle Hooks	Event Listeners
Purpose	Resource management (tables, buckets)	Business logic reactions
Sync Mode	@sync_collection_initializer	@sync_event_listener
Async Mode	@async_collection_destroyer	@async_event_listener
Config Access	Receives config snapshot	Receives event kwargs
Use Case	CREATE/DROP tables/buckets	Validation, notifications, indexing

## Testing

GeoID uses `pytest` for testing. We distinguish between **unit tests** (for libraries and tools) and **integration tests** (for APIs and cross-module flows).

### 1. Install Test Dependencies
To run the full test suite, you must install all dependencies. The recommended way is using the **Complete Installation** method described above:
```sh
# This installs everything needed for all unit and integration tests.
pip install -e '.[all]'
```

### 2. Local Unit Tests
You can run unit tests locally without spinning up the entire infrastructure if they don't depend on external services:
```sh
pytest tests/GeoID/modules/catalog/unit
```

### 3. Integration Tests (against Docker)
To run integration tests that require the API and Database:
1. Start the dev environment:
   ```sh
   ./manage.sh dev
   ```
2. Run the integration tests:
   ```sh
   pytest tests/GeoID/modules/catalog/integration
   ```

### 4. VS Code Integration
To integrate with VS Code's Testing plugin:
1. Ensure your local `.venv` is selected as the Python interpreter.
2. The `pytest.ini` file in the root will automatically configure discovery for both `tests/` and `src/` directories.
3. If not already enabled, add the following to your `.vscode/settings.json`:
   ```json
   {
       "python.testing.pytestEnabled": true,
       "python.testing.pytestArgs": ["-vv"]
   }
   ```

- CI/CD integration is recommended for production deployments.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

## Contact

Maintainer: carlo.cancellieri@fao.org
