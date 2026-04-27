# The Joins Extension

The `joins` extension implements a working-draft of **OGC API – Joins Part 1**
for DynaStore. It lets a client enrich a primary DynaStore collection (any
READ-capable storage driver) with attributes from a secondary source (another
registered collection or an ad-hoc BigQuery query), joining on a shared column
and returning the merged result as a feature stream.

The legacy `/dwh` endpoint (tightly coupled to BigQuery) remains supported and
installs automatically alongside `/join` when `dynastore[dwh]` is used. See
[§ Legacy DWH endpoint](#legacy-dwh-endpoint) below.

---

## URL structure

```
/join/                                                                           landing page
/join/conformance                                                                OGC conformance
/join/catalogs/{catalog_id}/collections/{collection_id}/join  GET  describe join capabilities
/join/catalogs/{catalog_id}/collections/{collection_id}/join  POST execute join → FeatureCollection
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-joins-1/0.0/conf/core
```

---

## Request body (`POST .../join`)

```json
{
  "secondary": { ... },
  "join": {
    "primary_column": "geoid",
    "secondary_column": "id",
    "enrichment": true
  },
  "primary_filter": {
    "cql": "status = 'active'",
    "cql_lang": "cql2-text"
  },
  "projection": {
    "with_geometry": true,
    "destination_crs": 4326,
    "attributes": ["name", "area_ha"]
  },
  "paging": { "limit": 100, "offset": 0 },
  "output": { "format": "geojson" }
}
```

### `secondary` — two variants

**`NamedSecondarySpec`** — reference a registered collection:
```json
{ "driver": "registered", "ref": "catalog_id/collection_id" }
```

**`BigQuerySecondarySpec`** — per-request BigQuery target:
```json
{
  "driver": "bigquery",
  "target": {
    "project_id": "my-project",
    "dataset": "my_dataset",
    "table": "my_table"
  }
}
```

### Parameters

| Field | Type | Default | Description |
|---|---|---|---|
| `join.primary_column` | `str` | required | Join key on primary collection |
| `join.secondary_column` | `str` | required | Join key on secondary source |
| `join.enrichment` | `bool` | `true` | Merge secondary properties into primary feature |
| `primary_filter.cql` | `str` | — | CQL2 expression applied to primary driver |
| `primary_filter.cql_lang` | `"cql2-text"\|"cql2-json"` | `"cql2-text"` | CQL2 dialect |
| `projection.with_geometry` | `bool` | `true` | Include geometry in output |
| `projection.destination_crs` | `int` | `4326` | Output EPSG code |
| `projection.attributes` | `list[str]` | all | Subset of primary attributes to return |
| `paging.limit` | `int` | `100` | Max features (1–10 000) |
| `paging.offset` | `int` | `0` | Page offset |
| `output.format` | `str` | `"geojson"` | `geojson`, `json`, `csv`, `geopackage`, `parquet` |

---

## Join pipeline

```
POST /join/.../join
        │
        ├─ 1. resolve_drivers("READ", catalog, collection, hint="features")
        │     → ordered list of primary READ drivers
        ├─ 2. resolve secondary source
        │     NamedSecondarySpec  → resolve_drivers("READ", secondary ref)
        │     BigQuerySecondarySpec → stream_bigquery_secondary(target)
        ├─ 3. executor.index_secondary(secondary_stream)
        │     → materialize secondary into dict {secondary_column → properties}
        ├─ 4. primary_driver.stream_items(cql_filter=primary_filter)
        │     → async iterator of Feature
        └─ 5. executor.run_join(primary_stream, secondary_index, join_spec, projection)
              → inner join: for each primary feature, lookup secondary_index[join_key]
              → if enrichment=true, merge secondary properties into feature.properties
              → StreamingResponse FeatureCollection + _join_meta
```

### Response envelope

```json
{
  "type": "FeatureCollection",
  "features": [ ... ],
  "_join_meta": {
    "secondary_rows_materialized": 42000,
    "joined_features": 1250,
    "secondary_ref": "catalog_a/collection_b"
  }
}
```

`_join_meta.secondary_rows_materialized` is the size of the secondary index held
in memory during the join. Large secondary tables should be filtered at source
before joining.

---

## Collection link contribution

`JoinsLinkContributor` injects a `rel="join"` link into collection metadata
responses, advertising the join endpoint URL for any collection that has at
least one READ driver registered.

---

## Key files

| File | Responsibility |
|---|---|
| `extensions/joins/joins_service.py` | FastAPI router; GET describe + POST execute (priority 130) |
| `extensions/joins/config.py` | `JoinsPluginConfig` — `enabled` flag only |
| `extensions/joins/link_contrib.py` | Adds `rel="join"` link to collection metadata |
| `modules/joins/executor.py` | Driver-agnostic inner join: `index_secondary()` + `run_join()` |
| `modules/joins/bq_secondary.py` | Wraps `ItemsBigQueryDriver` for per-request BQ targets |
| `modules/joins/models.py` | Pydantic request / response DTOs |

---

## Legacy DWH endpoint

`extensions/dwh/` is a pre-OGC-API-Joins surface that predates the `/join`
extension. It remains supported for backwards compatibility and is installed
automatically when `dynastore[dwh]` is used (which also installs `dynastore[joins]`).

| Route | Description |
|---|---|
| `POST /dwh/join` | Legacy join — catalog id in request body |
| `POST /dwh/catalogs/{catalog_id}/join` | Per-catalog variant |
| `POST /dwh/catalogs/{catalog_id}/tiles/{z}/{x}/{y}/join.{format}` | MVT tile join |

Key differences from `/join`:

- **BigQuery only** — secondary is always a BQ query string; no named secondary.
- **Raw SQL in body** — the `dwh_query` field is a full BigQuery SQL statement.
  The `where` field is disabled (SQL injection risk); structured CQL2 filtering
  is not wired up.
- **MVT tiling** — the tiled join endpoint clips features to a tile bbox and
  returns Mapbox Vector Tile (MVT/PBF) content. The `/join` extension has no
  tiled variant yet.
- **No OGC conformance** — `/dwh` is not an OGC API endpoint and declares no
  conformance URIs.

Migration path: replace `POST /dwh/catalogs/{id}/join` with
`POST /join/catalogs/{id}/collections/{col}/join` using a
`BigQuerySecondarySpec` secondary and a `primary_filter` in CQL2.

---

## Packaging

```toml
joins = ["google-cloud-bigquery", "db-dtypes", "pandas"]
dwh   = ["dynastore[joins]"]
```

`pip install dynastore[dwh]` installs both `/dwh` and `/join` endpoints.
`pip install dynastore[joins]` installs `/join` only.

---

## Known limitations

1. **Inner join only** — features in the primary stream with no matching row in
   the secondary index are dropped. LEFT JOIN semantics are planned but not yet
   implemented.
2. **Secondary materialized in memory** — the full secondary source is indexed
   before streaming the primary. Very large secondary tables can exhaust memory;
   pre-filter at source.
3. **CQL2 filter requires driver support** — primary drivers that do not
   implement CQL2 parsing treat `primary_filter` as a no-op.
4. **No paging on secondary** — `NamedSecondarySpec` materializes all secondary
   rows without a limit.
5. **BigQuerySecondarySpec credentials** — per-request BQ targets use
   `CloudIdentityProtocol` (ADC); Secret-wrapped per-collection credentials are
   not yet supported for secondary specs.
6. **No streaming output formats** — GeoPackage and Parquet outputs accumulate
   the full result in memory before writing.
