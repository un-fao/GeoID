# The Styles Extension

The `styles` extension implements **OGC API – Styles Part 1** for DynaStore.
It provides full CRUD management for per-collection styles (SLD 1.1,
Mapbox GL), content-negotiated stylesheet retrieval, and cross-catalog style
discovery. Styles integrate into the Maps and Coverages rendering pipelines via
a `StylesResolver` and a `StylesLinkContributor`.

---

## URL structure

```
/styles/                                                                            landing page
/styles/conformance                                                                 OGC conformance
/styles/all                                                                         cross-catalog style list
/styles/catalogs/{catalog_id}/collections/{collection_id}/styles                    list / create styles
/styles/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}         get / update / delete
/styles/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}/stylesheet    content-negotiated body
/styles/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}/metadata      metadata + links
/styles/catalogs/{catalog_id}/collections/{collection_id}/styles/{style_id}/legend        307 → preview link
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/manage-styles
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/style-info
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/mapbox-style
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-10
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-11
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/html
http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/json
```

---

## Database schema

```sql
CREATE SCHEMA IF NOT EXISTS styles;

CREATE TABLE IF NOT EXISTS styles.styles (
    id            UUID DEFAULT gen_random_uuid(),
    catalog_id    VARCHAR NOT NULL,
    collection_id VARCHAR NOT NULL,
    style_id      VARCHAR NOT NULL,         -- user-defined, unique within collection
    title         VARCHAR,
    description   TEXT,
    keywords      TEXT[],
    stylesheets   JSONB,                    -- array of {content: {format, ...}}
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (catalog_id, id),
    UNIQUE (catalog_id, collection_id, style_id)
) PARTITION BY LIST (catalog_id);
```

Partitions are created on-demand when a new `catalog_id` is first seen
(`ensure_partition_exists` in the POST handler).

---

## Supported style formats

| `?f=` shorthand | Media type | Pydantic model | Status |
|---|---|---|---|
| `mapbox` / `json` | `application/json` | `MapboxContent` | Fully implemented |
| `sld11` | `application/vnd.ogc.sld+xml;version=1.1` | `SLDContent` (lxml validated) | Fully implemented |
| `sld10` | `application/vnd.ogc.sld+xml` | — | Declared; no content model |
| `qml` | `application/xml` | — | Declared; no content model |
| `flat` | `application/vnd.ogc.flat-style+json` | — | Declared; no content model |
| `html` | `text/html` | — | Declared; no content model |

Multiple encodings can be stored per style record (one `StyleSheet` entry per
encoding in `stylesheets` JSONB). The `/stylesheet` endpoint picks via
content negotiation (Accept header q-value weighting with base-type fallback,
or `?f=` override).

---

## CRUD pipeline

### Create style (POST)

```
POST /styles/catalogs/{cat}/{coll}/styles
        │   body: StyleCreate JSON
        │
        ├─ 1. validate_sql_identifier(catalog_id, collection_id)
        ├─ 2. catalog_module.get_collection(cat, coll) → 404 if missing
        ├─ 3. ensure_partition_exists("styles", strategy=LIST, partition_value=cat)
        ├─ 4. styles_db.create_style()
        │     → INSERT INTO styles.styles … RETURNING *
        │     → _enrich_style_from_row() injects /stylesheet + self links
        └─ 5. 201 Created (Style JSON)
```

### Retrieve stylesheet (GET /stylesheet)

```
GET /styles/.../stylesheet          Accept: application/json
        │
        ├─ 1. get_style_by_id_and_collection(cat, coll, style_id)
        ├─ 2. Content negotiation:
        │     a. if ?f= present → f_param_to_media_type(f)
        │     b. else → normalize_accept_to_media_type(Accept, available_types)
        │        (q-value weighted; base-type fallback)
        ├─ 3. _pick_stylesheet_by_media_type(style, target)
        └─ 4. _stylesheet_to_bytes(sheet)
              SLDContent  → sheet.content.sld_body.encode("utf-8")
              MapboxContent → json.dumps(model_dump(by_alias=True, exclude_none=True))
              → Response(bytes, media_type=target)
```

### Update style (PUT)

Builds a dynamic `UPDATE … SET col=:col` from `update_values` keys (Pydantic
field names only — injection risk is low but the approach is not
parameterised-identifier safe). Requires two round-trips: lookup by
`style_id` → update by internal UUID.

Key files:

| File | Responsibility |
|---|---|
| `extensions/styles/styles_service.py` | Router, orchestration (priority 100) |
| `extensions/styles/config.py` | `StylesPluginConfig` (`enabled` only) |
| `modules/styles/styles_module.py` | Startup DDL — schema + partitioned table |
| `modules/styles/db.py` | All SQL as `DQLQuery`/`DDLQuery` objects |
| `modules/styles/models.py` | `StyleFormatEnum`, `SLDContent`, `MapboxContent`, `Style`, `StyleCreate`, `StyleUpdate` |
| `modules/styles/encodings.py` | Media-type constants, `f_param_to_media_type()`, `normalize_accept_to_media_type()` |
| `modules/styles/resolver.py` | `StylesResolver` — pure precedence cascade (no DB) |
| `modules/styles/link_contrib.py` | `StylesLinkContributor` — emits `rel=styles`/`rel=style` links |
| `modules/styles/collection_pipeline.py` | `StylesCollectionPipeline` — merges default style into `item_assets` |

---

## Query parameters

| Parameter | Type | Default | Endpoints | Description |
|---|---|---|---|---|
| `catalog_id` | str | required | all collection routes | path |
| `collection_id` | str | required | all collection routes | path |
| `style_id` | str | required | single-style routes | path |
| `limit` | int | 100 | list, list_all | Max styles (1–1000) |
| `offset` | int | 0 | list, list_all | Pagination offset |
| `f` | str | none | get_stylesheet | Format shorthand; overrides Accept |

---

## Integration with Maps and Coverages

`StylesResolver` provides a pure-function precedence cascade with no DB
access. It resolves the "default style" from:
1. `CoveragesConfig.default_style_id` (highest priority)
2. STAC `item_assets` default annotation
3. `None`

`StylesLinkContributor` (discovered via `CollectionPipelineProtocol`) appends
`rel=styles` and per-encoding `rel=style` links to collection asset link sets.

`StylesCollectionPipeline` merges the resolved default style into
`item_assets["default_style"]` (author-provided values win).

---

## Dependencies

| Package | Source | Purpose |
|---|---|---|
| `lxml>=6.1.0,<7` | `geospatial_io` | SLD XML validation (`etree.fromstring`) |
| `sqlalchemy[asyncio]` / `asyncpg` | core | Async DB access |

---

## Known limitations

1. **`sld10`, `qml`, `flat`, `html` not backed by content models** — these
   format shortcuts are declared in `encodings.py` but there are no
   `StyleSheet` content model variants; requesting them returns 406.
2. **Dynamic SET clause in update** — `db.py` interpolates Pydantic field
   names into SQL; not parameterised-identifier safe for future fields with
   special characters.
3. **Delete requires two round-trips** — lookup by `style_id` → delete by
   internal UUID.
4. **`list_all_styles` is a cross-partition scan** — no catalog/collection
   filter; may be slow in large deployments.
5. **Legend endpoint is a redirect only** — 307 to the `rel=preview` link
   stored in the style; no server-side legend generation.
6. **`sample_qml_style.qml` is unreferenced** — stored in the module directory
   but never imported or tested.
