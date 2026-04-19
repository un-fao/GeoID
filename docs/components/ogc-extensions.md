# OGC API Extensions

DynaStore exposes a growing surface of OGC API extensions, each registered via
the `dynastore.extensions` entry-point group. This page is the reader-facing
map of what's wired and how the pieces relate.

## Surface

| Extension | URL prefix | Module | Conformance highlights |
|---|---|---|---|
| Records | `/records` | `extensions/records/` | Core, OAS 3.0/3.1, GeoJSON encoding |
| Web | `/web` | `extensions/web/` | OAS 3.0 + 3.1 (`?f=oas31` on `/api`) |
| Maps | `/maps` | `extensions/maps/` | PNG default, JPEG + GeoTIFF (`?f=jpeg|geotiff` on `/map`) |
| Styles | `/styles` | `extensions/styles/` | Core, manage-styles, style-info, MapboxGL, SLD-1.0/1.1, HTML, JSON; content-negotiated `/stylesheet`, `/metadata`, `/legend`, root `/styles` list |
| Coverages | `/coverages` | `extensions/coverages/` | Core, geodata-coverage, JSON, HTML, subset/bbox/datetime, GeoTIFF/NetCDF/CoverageJSON encodings |
| Processes | `/processes` | `extensions/processes/` | Part 1 Core, ogc-process-description, JSON, job-list, dismiss; sync (`Prefer: respond-sync`) + async dispatch |
| 3D GeoVolumes | `/volumes` | `extensions/volumes/` | Draft URIs (core, 3dtiles, tileset); `tileset.json` route shell, `/tiles/{id}.b3dm` 501 pending writer phase, `/metadata` |
| Joins | `/join` | `extensions/joins/` | Draft URI (core); discriminated `secondary` (NamedSecondarySpec / BigQuerySecondarySpec); resolves both sides via `resolve_drivers("READ", ..., hint="features")` |

The legacy `/dwh/*` extension (tile-join MVT/PBF surface) lives at
`extensions/dwh/` and remains supported. `/join/*` is the OGC-conformant
replacement; both ship together under the same packaging extra (see below).

## Packaging

Extensions are grouped by deployable scope via `pyproject.toml` extras. Each
scope corresponds to one pip install target (`dynastore[<scope>]`).

```toml
joins = ["google-cloud-bigquery", "db-dtypes", "pandas"]
dwh   = ["dynastore[joins]"]   # installing dwh pulls joins automatically
```

`pip install dynastore[dwh]` registers BOTH `DwhService` and `JoinsService`.
This lets the OGC `/join/*` surface coexist with the legacy `/dwh/*`
endpoints during migration without forcing a deployment to choose.

## Architectural seams

- **`OGCServiceMixin`** (`extensions/ogc_base.py`) — the shared base for every
  OGC extension. Provides `_get_catalogs_service()`, `_get_configs_service()`,
  `ogc_landing_page_handler()`, `ogc_conformance_handler()`,
  `register_policies()` (override point). Conformance URIs are contributed via
  the `conformance_uris` class attr; the global `/conformance` aggregator
  walks every registered extension and collects them.
- **`OGCTransactionMixin`** — multi-item ingest with 207 IngestionReport
  responses (used by Features, STAC, Records).
- **`CollectionPipelineProtocol`** (`models/protocols/`) — per-collection
  rewrite pipeline (drop / replace / transform) consumed by STAC + Features
  collection endpoints.
- **`LinkContributor`** + **`AnchoredLink`** — extensions inject anchored
  links (e.g. style links on STAC items) without coupling producer to
  consumer.
- **`StylesResolver`** — central precedence cascade for default style id
  (`CoveragesConfig.default_style_id` > STAC item-assets default > none).
- **`BoundsSourceProtocol`** (`models/protocols/bounds_source.py`) — pluggable
  source of 3D feature bounding boxes for the volumes tile-hierarchy builder.
  Default `EmptyBoundsSource` keeps the `/volumes/*` surface working when no
  real producer is registered.
- **Storage drivers** (`modules/storage/drivers/*.py`) — Postgres, DuckDB,
  Iceberg, Elasticsearch, BigQuery. Each implements
  `CollectionItemsStore.read_entities` (and write/lifecycle methods where
  capable). `resolve_drivers("READ", catalog, collection, hint="features")`
  walks the platform routing config and returns an ordered list.

## Adding a new OGC extension

1. New package at `src/dynastore/extensions/<name>/`.
2. Service class subclassing `ExtensionProtocol` + `OGCServiceMixin`, with
   `prefix = "/<name>"` and `conformance_uris = [...]`.
3. Register routes in `_register_routes()`.
4. Add a `[project.entry-points."dynastore.extensions"]` line in
   `pyproject.toml` (and a packaging extra if the extension needs deps).
5. Pin conformance URIs via the AST snapshot test in
   `tests/dynastore/extensions/test_conformance_snapshot.py` — the helper
   reads source files via `ast` so the test runs without importing modules
   that need heavy deps (e.g. osgeo, rasterio).

`extensions/coverages/coverages_service.py` is the closest reference for the
service-class shape; `extensions/volumes/volumes_service.py` shows the
minimal-stub variant.

## Build / test conventions

- Local venv (`.venv/`) lacks heavy deps (`osgeo`, `rasterio`, `netCDF4`,
  `trimesh`); production GDAL base image has them. Tests touching these use
  `importlib.util.find_spec(...)` + `pytest.mark.skipif`.
- Lint: `uv tool run ruff check`. Types: `.venv/bin/pyright`. Tests:
  `.venv/bin/python -m pytest`.
- Conformance URI snapshot tests use `_read_uri_list_from_source(...)` (AST
  parser) so they run without importing the service module. Use this pattern
  for any new conformance list.
