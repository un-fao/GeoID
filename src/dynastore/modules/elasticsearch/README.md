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

| Variable | Default | Description |
|---|---|---|
| `ES_HOST` | `localhost` | ES/OpenSearch hostname or IP |
| `ES_PORT` | `9200` | REST API port |
| `ES_USE_SSL` | `false` | `true` to use HTTPS (Elastic Cloud, on-prem TLS) |
| `ES_VERIFY_CERTS` | `true` | `false` to skip TLS certificate verification (self-signed) |
| `ES_API_KEY` | — | API key authentication (preferred over basic auth) |
| `ES_USERNAME` | — | Basic auth username |
| `ES_PASSWORD` | — | Basic auth password |
| `ES_INDEX_PREFIX` | `dynastore` | Prefix applied to all index names |
| `ES_CONNECTIONS_PER_NODE` | `10` | Connection pool size per node |

#### On-Premise / OpenSearch Example

```env
ES_HOST=my-opensearch-node.internal
ES_PORT=9200
ES_USE_SSL=true
ES_VERIFY_CERTS=false   # self-signed cert
ES_API_KEY=abc123==
ES_INDEX_PREFIX=geoid_prod
```

#### Startup Connectivity Check

On startup, `client.init()` attempts a ping (`GET /`) against the configured host.
On success the cluster name and version are logged at `INFO`. On failure a `WARNING` is
emitted and the service continues — ES is optional. Indexing tasks will fail (and retry)
until the connection is restored.

#### License Note

This module is compatible with **OpenSearch 2.x** (Apache 2.0) and
**Elasticsearch ≤7.10.2** (Apache 2.0). Elasticsearch ≥7.11 uses SSPL/ELv2,
which is incompatible with this project's Apache 2.0 license.
The Docker Compose files in this repository use
`opensearchproject/opensearch:2.17.0` for this reason.

### Per-Catalog Config (runtime-mutable, stored in AlloyDB)

Each catalog can have an `ElasticsearchCatalogConfig` stored via the standard
configuration API:

```
PUT /configs/catalogs/{catalog_id}/elasticsearch
{ "private": true }
```

| Field | Type | Default | Description |
|---|---|---|---|
| `private` | `bool` | `false` | When `true`, items are indexed in geoid-only mode and all GET access is blocked for `all_users` via a DENY policy. |

This config is registered via `@register_config("elasticsearch", on_apply=...)` and
follows the same pattern as `gcp_catalog_bucket`. The `on_apply` callback fires on
every write — toggling private mode does not require a restart.

## Index Design & Mappings

The module automatically configures and manages the following Elasticsearch indices:

| Index | Entity | Key mappings |
|---|---|---|
| `{prefix}-catalogs` | Catalog | keyword + multilingual free-text |
| `{prefix}-collections` | Collection | `geo_shape` (spatial extent), date range (temporal) |
| `{prefix}-items` | Item | `geo_shape` (geometry), STAC datetime, dynamic template |
| `{prefix}-assets` | Asset | `item_id`, `asset_key`, `roles`, `href` |
| `{prefix}-geoid-{catalog_id}` | Private item | `dynamic: false`, `geoid`, `catalog_id`, `collection_id` only |

## GeoID Private Mode

When a catalog's ES config has `private: true`:

1. A **DENY policy** blocks all GET requests across all protocol paths for the catalog.
2. A **geoid-only index** (`{prefix}-geoid-{catalog_id}`) is created with minimal mapping.
3. Items are indexed as `{geoid, catalog_id, collection_id}` — no geometry, no STAC metadata.
4. The STAC items index is **never populated** for private catalogs.
5. At startup, the module scans all catalogs and **restores in-memory DENY policies**.
6. Geoid lookups are available via `GET /search/geoid/{geoid}` and `POST /search/geoid` (batch).

See `docs/components/elasticsearch.md` for the full lifecycle description.

## Tasks

### Per-item (worker, incremental)
- `elasticsearch_index` — index a full STAC document
- `elasticsearch_delete` — delete a document (safe on NotFoundError)
- `elasticsearch_private_index` — index one geoid-only document
- `elasticsearch_private_delete` — delete one geoid document

### Bulk (Cloud Run Job or worker)
- `elasticsearch_bulk_reindex_catalog` — reindex all items in a catalog (500-doc batches)
- `elasticsearch_bulk_reindex_collection` — reindex one collection

Bulk tasks are triggered by the admin endpoint `POST /search/reindex/catalogs/{id}`.

## Dependencies

The module requires the official Elasticsearch async Python client:
```bash
poetry add elasticsearch[async]
```
