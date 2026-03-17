# Elasticsearch Indexer Tasks

Bulk reindex and obfuscated index tasks for the Elasticsearch integration.

## Task Types

### Bulk Reindex (Cloud Run Job or worker)

| Task type | Inputs | Description |
|---|---|---|
| `elasticsearch_bulk_reindex_catalog` | `catalog_id`, `mode` | Stream all collections/items, bulk-index in 500-doc batches |
| `elasticsearch_bulk_reindex_collection` | `catalog_id`, `collection_id`, `mode` | Same for a single collection |

Mode values (`Literal["catalog", "obfuscated"]`):
- `"catalog"` — index full STAC documents into `{prefix}-items`; skips collections with `search_index=False`; cleans stale obfuscated docs.
- `"obfuscated"` — index geoid-only documents into `{prefix}-geoid-{catalog_id}`; cleans stale STAC items for the catalog.

Triggered by `POST /search/reindex/catalogs/{id}` (admin endpoint).

### Per-item Obfuscated (worker, incremental)

| Task type | Inputs | Description |
|---|---|---|
| `elasticsearch_obfuscated_index` | `geoid`, `catalog_id`, `collection_id` | Index one `{geoid, catalog_id, collection_id}` doc |
| `elasticsearch_obfuscated_delete` | `geoid`, `catalog_id` | Delete one geoid doc (safe on NotFoundError) |

Dispatched by `ElasticsearchModule` event handlers when a catalog has `obfuscated=True`.

## Cloud Run Job

The `geospatial-elasticsearch-indexer` Cloud Run Job handles bulk reindex for large catalogs:

```yaml
geospatial-elasticsearch-indexer:
  type: "job"
  env:
    SCOPE: "task-elasticsearch-indexer-job"
    TASK_TIMEOUT: 7200    # 2 hours
    RAM: "2Gi"
    MAX_RETRIES: 2
```

## Files

```
__init__.py    # Exports task classes
tasks.py       # BulkCatalogReindexTask, BulkCollectionReindexTask,
               # ObfuscatedIndexTask, ObfuscatedDeleteTask
```
