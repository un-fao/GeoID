# Elasticsearch Integration Module

This module automatically indexes DynaStore entities (Catalogs, Collections, Items, Assets) into an Elasticsearch cluster, allowing for highly optimized free-text, spatial, and temporal searches.

## Protocols

The module implements `IndexerProtocol` (`models/protocols/indexer.py`), exposing a backend-agnostic contract for document indexing. Other components discover it via `get_protocol(IndexerProtocol)` — no direct imports needed. The companion search extension implements `SearchProtocol` (`models/protocols/search.py`), also discovered at runtime.

To swap to a different backend, implement the same protocols in a new module/extension.

## Architecture

The module leverages DynaStore's asynchronous, event-driven architecture and durable task queue to ensure reliability and performance.
1. **Event Emission:** Core services (CatalogService, CollectionService, etc.) emit events (e.g. `CATALOG_CREATION`, `ITEM_CREATION`, `BULK_ITEM_CREATION`) during their database transactions.
2. **Event Listening:** The `ElasticsearchModule` listens to these events using the generic `EventsProtocol`.
3. **Task Enqueuing:** Upon receiving an event, the module immediately enqueues a *durable background task* to the `TasksModule` queue and returns control, ensuring the HTTP request is not blocked by Elasticsearch network operations.
4. **Task Execution & Retries:** Background workers execute the indexing tasks (`ElasticsearchIndexTask`, `ElasticsearchDeleteTask`). If Elasticsearch is temporarily unavailable, the task is automatically retried with exponential backoff.

## Configuration

The Elasticsearch module is installed as part of the `elasticsearch` extra. Enable it
by installing the correct extras for your deployment:

```bash
pip install dynastore[elasticsearch]
# or via SCOPE in your deployment config:
# SCOPE=elasticsearch
```

### Connection (environment variables, deploy-time)

```env
ELASTICSEARCH_URL="http://localhost:9200"
ELASTICSEARCH_API_KEY=""            # Optional
ELASTICSEARCH_USERNAME=""           # Optional
ELASTICSEARCH_PASSWORD=""           # Optional
ELASTICSEARCH_VERIFY_CERTS="true"
ELASTICSEARCH_INDEX_PREFIX="dynastore"
```

### Per-Catalog Config (runtime-mutable, stored in AlloyDB)

Each catalog can have an `ElasticsearchCatalogConfig` stored via the standard
configuration API:

```
PUT /configs/catalogs/{catalog_id}/elasticsearch
{ "obfuscated": true }
```

| Field | Type | Default | Description |
|---|---|---|---|
| `obfuscated` | `bool` | `false` | When `true`, items are indexed in geoid-only mode and all GET access is blocked for `all_users` via a DENY policy. |

This config is registered via `@register_config("elasticsearch", on_apply=...)` and
follows the same pattern as `gcp_catalog_bucket`. The `on_apply` callback fires on
every write — toggling obfuscated mode does not require a restart.

## Index Design & Mappings

The module automatically configures and manages the following Elasticsearch indices:

| Index | Entity | Key mappings |
|---|---|---|
| `{prefix}-catalogs` | Catalog | keyword + multilingual free-text |
| `{prefix}-collections` | Collection | `geo_shape` (spatial extent), date range (temporal) |
| `{prefix}-items` | Item | `geo_shape` (geometry), STAC datetime, dynamic template |
| `{prefix}-assets` | Asset | `item_id`, `asset_key`, `roles`, `href` |
| `{prefix}-geoid-{catalog_id}` | Obfuscated item | `dynamic: false`, `geoid`, `catalog_id`, `collection_id` only |

## GeoID Obfuscated Mode

When a catalog's ES config has `obfuscated: true`:

1. A **DENY policy** blocks all GET requests across all protocol paths for the catalog.
2. A **geoid-only index** (`{prefix}-geoid-{catalog_id}`) is created with minimal mapping.
3. Items are indexed as `{geoid, catalog_id, collection_id}` — no geometry, no STAC metadata.
4. The STAC items index is **never populated** for obfuscated catalogs.
5. At startup, the module scans all catalogs and **restores in-memory DENY policies**.
6. Geoid lookups are available via `GET /search/geoid/{geoid}` and `POST /search/geoid` (batch).

See `docs/components/elasticsearch.md` for the full lifecycle description.

## Tasks

### Per-item (worker, incremental)
- `elasticsearch_index` — index a full STAC document
- `elasticsearch_delete` — delete a document (safe on NotFoundError)
- `elasticsearch_obfuscated_index` — index one geoid-only document
- `elasticsearch_obfuscated_delete` — delete one geoid document

### Bulk (Cloud Run Job or worker)
- `elasticsearch_bulk_reindex_catalog` — reindex all items in a catalog (500-doc batches)
- `elasticsearch_bulk_reindex_collection` — reindex one collection

Bulk tasks are triggered by the admin endpoint `POST /search/reindex/catalogs/{id}`.

## Dependencies

The module requires the official Elasticsearch async Python client:
```bash
poetry add elasticsearch[async]
```
