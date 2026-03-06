# Elasticsearch Integration Module

This module automatically indexes DynaStore entities (Catalogs, Collections, Items, Assets) into an Elasticsearch cluster, allowing for highly optimized free-text, spatial, and temporal searches.

## Architecture

The module leverages DynaStore's asynchronous, event-driven architecture and durable task queue to ensure reliability and performance.
1. **Event Emission:** Core services (CatalogService, CollectionService, etc.) emit events (e.g. `AFTER_CATALOG_CREATION`) during their database transactions.
2. **Event Listening:** The `ElasticsearchModule` listens to these events using the generic `EventsProtocol`.
3. **Task Enqueuing:** Upon receiving an event, the module immediately enqueues a *durable background task* to the `TasksModule` queue and returns control, ensuring the HTTP request is not blocked by Elasticsearch network operations.
4. **Task Execution & Retries:** Background workers execute the indexing tasks (`ElasticsearchIndexTask`, `ElasticsearchDeleteTask`). If Elasticsearch is temporarily unavailable, the task is automatically retried with exponential backoff.

## Configuration

Set the following environment variables to configure the module:

```env
# Enable the module
DYNASTORE_MODULES="...,elasticsearch"

# Enable task workers to run the ES tasks
DYNASTORE_TASK_MODULES="...,elasticsearch"

# Elasticsearch Connection
ELASTICSEARCH_URL="http://localhost:9200"
ELASTICSEARCH_API_KEY="" # Optional
ELASTICSEARCH_USERNAME="" # Optional
ELASTICSEARCH_PASSWORD="" # Optional
ELASTICSEARCH_VERIFY_CERTS="true"

# Index Prefixes
ELASTICSEARCH_INDEX_PREFIX="dynastore"
```

## Index Design & Mappings

The module automatically configures and manages the following Elasticsearch indices:

*   **`dynastore-catalogs`**: Stores catalog metadata. Mapped for keyword and free-text searches on title, description, and tags.
*   **`dynastore-collections`**: Stores collection metadata. Includes `geo_shape` mapping for the spatial extent and `date_range` for the temporal extent.
*   **`dynastore-items-*`**: Item indices (often datastreams or partitioned by time/collection depending on configuration). Includes `geo_shape` for geometry, `date` for STAC `datetime`, and a *dynamic template* for STAC `properties`. This dynamic template ensures all custom properties are mapped as keywords or numerics without causing mapping explosions, enabling robust aggregations and filtering.

## Dependencies

The module requires the official Elasticsearch async Python client:
```bash
poetry add elasticsearch[async]
```
