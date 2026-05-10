# Admin Boundaries — Fixed-Schema Columnar Walkthrough

End-to-end walkthrough demonstrating the recipe described in
[issue #447](https://github.com/un-fao/GeoID/issues/447):

1. Collection optimised for **columnar / OTF** storage (DuckDB or Iceberg)
2. **Fixed schema** derived from `gdalinfo` on a sample shapefile / GeoJSON
3. GCS uploads with the **same schema** and a **distinct `asset_id` per file**
   (duplicates refused at DB level via `assets_identity_uq`)
4. **`external_id = "code"` mandatory** (`require_external_id=true`,
   `external_id_field="properties.code"`)
5. **2-D geometry statistics** (area, length, centroid, circularity, …)
6. **Geometry fix** (`invalid_geometry_policy=attempt_fix`,
   `target_dimension=force_2d`, `srid_mismatch_policy=transform`)
7. **`GcsDetailedReporter`** writing JSONL to a separate scratch bucket
8. PG-sidecar tiles, cached on bucket
9. **STAC + OGC Features search via Elasticsearch**
   (`geometry_simplified` hint, default since PR #185)

## Files

| Path | Role |
|---|---|
| `walkthrough.ipynb` | Single end-to-end notebook (lift-cells composition of 7 sources) |
| `fixtures/admin_boundaries.geojson` | 3-feature country sample with `code` external-id field |

## Source notebooks

The cells were lifted from the showcase suite on branch
`worktree-sidecars-collapse-and-snapshot` under `notebook_showcase/notebooks/`:

| Step | Source |
|---|---|
| 0 — gdalinfo → schema   | `stac_extensions/01_gdal_to_stac_transformer.ipynb` |
| 1 — catalog             | `_setup/00_ensure_demo_catalog.ipynb` |
| 2 — collection sidecars | `catalog/02_create_collection_with_layerconfig.ipynb` + `write_policy/01_external_id_deduplication.ipynb` |
| 3 — routing             | `routing/01_pg_primary_es_index_parquet_backup.ipynb` |
| 4 — bucket + Pub/Sub    | `gcp/01_bucket_init_upload_and_ingest.ipynb` |
| 5 — reporter            | `ui_walkthrough/02_upload_with_reporter.ipynb` |
| 6 — search + tiles      | `ui_walkthrough/03_read_search_features_tiles.ipynb` + `queryables/01_cql2_and_collection_search.ipynb` |

## Environment

Set in `.env` (or export before launching the kernel):

```bash
DYNASTORE_BASE_URL=https://data.review.fao.org/geospatial/v2/api
DYNASTORE_SYSADMIN_TOKEN=<sysadmin bearer>
# optional
CATALOG_ID=admin-boundaries-demo
COLLECTION_ID=countries
REPORT_BUCKET=geoid-review-temp        # scratch bucket, NOT the catalog bucket
```

Local dev pointed at `http://localhost:8080` works for everything except the
GCS upload + Pub/Sub steps (the notebook auto-skips with `_GCP_ENABLED=False`).

## Platform gaps (deferred — not blocking this walkthrough's PR)

The notebook documents three gaps that need real platform work before the
walkthrough can be promised to customers without caveats. Each is called out in
a `**Gap A/B/C**` markdown cell at the relevant step:

- **Gap A** — no `bootstrap-schema` endpoint; step 0 is a 2-step copy/paste
  from `tasks/gdal/gdalinfo_task.py` output into
  `CollectionPluginConfig.sidecars[*].attribute_schema`.
- **Gap B** — OTF as **WRITE-primary** is not live-tested in review env;
  the notebook keeps PG as primary and notes how to swap.
- **Gap C** — GCS tile-cache config + observability lives in
  `fao-maps-titiler` and is not surfaced through geoid's config API.

Track these as separate issues if/when they are approved as features.
