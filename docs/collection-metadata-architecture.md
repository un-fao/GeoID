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

Layer 3: External sources via alternative CollectionStore drivers
  Per-collection routing selects which driver implementation reads metadata
  Drivers replace the old enricher chain — one mechanism, one SLA
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

## Layer 2: Metadata Tables

All descriptive metadata lives in two domain-scoped tables:
`collection_core` (format-agnostic identity + extent + lifecycle)
and `collection_stac` (STAC-specific summaries, providers, links).
DDL authored in `modules/catalog/db_init/core_tables.py` and
`modules/catalog/db_init/stac_tables.py`. The deployment policy is
delete-and-rebuild; no in-place migration is carried.

```sql
CREATE TABLE IF NOT EXISTS {schema}.collection_metadata (
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

## Layer 3: External Sources via CollectionStore

The `CollectionMetadataEnricherProtocol` runtime pipeline was removed.
External-source metadata (BigQuery row counts, ISO 19115 overlays, role-
filtered views) is now provided by alternative `CollectionStore` driver
implementations selected per-collection through `RoutingConfig` (see
`modules/routing/`). The canonical replacement pattern lives at
`modules/gcp/bq_collection_enricher.py`: a `CollectionStore` subclass
returning enriched rows from BigQuery. One mechanism, one priority
model, one SLA — no separate enricher chain.

## Configuration

Per-collection metadata sources are configured via `collection_configs`:

```python
from dynastore.modules.catalog.config_service import get_collection_config

config = await get_collection_config(catalog_id, collection_id, "iso19115")
# Returns: {"csw_url": "...", "record_id": "..."}
```

Enrichers use `can_enrich()` to check if a config entry exists for their plugin_id, enabling per-collection activation without global flags.
