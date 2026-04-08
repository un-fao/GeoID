# Collection Metadata Architecture

## The Three-Layer Model

DynaStore separates collection identity from metadata, enabling pluggable metadata sources.

```
Layer 1: collections table (PG)
  Identity only: id, catalog_id, created_at, updated_at, deleted_at
  Format-agnostic: works for STAC, OGC Records, or any future format

Layer 2: metadata table (PG, default source)
  STAC/descriptive metadata stored as JSONB columns
  Dedicated columns: title, description, keywords, license, extent,
    providers, summaries, links, assets, item_assets, stac_version,
    stac_extensions
  Catch-all: extra_metadata JSONB for arbitrary STAC extension fields

Layer 3: CollectionMetadataEnricherProtocol (runtime pipeline)
  Runs after PG metadata is read
  Enrichers registered via register_plugin(), discovered via get_protocols()
  Priority-ordered: lower priority runs first
```

## Layer 1: Collections Table

Lightweight registry with no format-specific columns:

```sql
CREATE TABLE IF NOT EXISTS {schema}.collections (
    id VARCHAR NOT NULL,
    catalog_id VARCHAR NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY (id)
);
```

## Layer 2: Metadata Table

All descriptive metadata lives here. Renamed from `pg_collection_metadata` to `metadata` in v1.0.

```sql
CREATE TABLE IF NOT EXISTS {schema}.metadata (
    collection_id VARCHAR NOT NULL PRIMARY KEY,
    title JSONB,
    description JSONB,
    keywords JSONB,
    license JSONB,
    extent JSONB,
    providers JSONB,
    summaries JSONB,
    links JSONB,
    assets JSONB,
    item_assets JSONB,
    stac_version VARCHAR(20) DEFAULT '1.1.0',
    stac_extensions JSONB DEFAULT '[]'::jsonb,
    extra_metadata JSONB
);
```

### Multilanguage Support

JSONB columns support `Internationalized[T]` values:
```json
{"en": "Temperature", "fr": "Temperature", "es": "Temperatura"}
```

### STAC Extensions

Fields from STAC extensions (datacube, eo, raster, etc.) are stored in `extra_metadata`:
```json
{"cube:dimensions": {...}, "eo:bands": [...]}
```

## Layer 3: Enricher Protocol

```python
from dynastore.models.protocols.enrichment import CollectionMetadataEnricherProtocol

class MyEnricher:
    enricher_id = "my_enricher"
    priority = 50  # lower runs first

    def can_enrich(self, catalog_id: str, collection_id: str) -> bool:
        """Return True if this enricher applies to the given collection."""
        ...

    async def enrich(self, catalog_id, collection_id, metadata, context):
        """Modify and return the metadata dict."""
        metadata["description"] = await fetch_external_description(collection_id)
        return metadata
```

Register during module lifespan:
```python
from dynastore.tools.discovery import register_plugin
register_plugin(MyEnricher())
```

### Enricher Pipeline Execution

Located in `collection_service.py`, the pipeline runs after metadata is read from PG:

1. `get_protocols(CollectionMetadataEnricherProtocol)` discovers all registered enrichers
2. Enrichers sorted by `priority` (ascending)
3. Each enricher's `can_enrich()` checked
4. Applicable enrichers called sequentially, each receiving the output of the previous

### Use Cases

| Scenario | How It Works |
|----------|-------------|
| Pure STAC collection | `metadata` table stores everything. No enrichers needed. |
| STAC extensions | `extra_metadata` JSONB column stores extension fields |
| Multilanguage | JSONB columns support `{"en": "...", "fr": "..."}` |
| Collection-level assets | `assets` JSONB column in `metadata` table |
| ISO 19115 merge | Enricher reads external catalog, overlays fields |
| BigQuery stats | Enricher injects real-time row counts into summaries |
| Computed extent | Enricher recalculates temporal extent from event log |
| Role-based filtering | Enricher strips fields based on user permissions |

## Configuration

Per-collection metadata sources are configured via `collection_configs`:

```python
from dynastore.modules.catalog.config_service import get_collection_config

config = await get_collection_config(catalog_id, collection_id, "iso19115")
# Returns: {"csw_url": "...", "record_id": "..."}
```

Enrichers use `can_enrich()` to check if a config entry exists for their plugin_id, enabling per-collection activation without global flags.
