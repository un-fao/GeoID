# The Coverages Extension

The `coverages` extension implements **OGC API – Coverages Part 1** for
DynaStore. It exposes raster data stored as STAC item assets in GeoTIFF,
NetCDF-4, Zarr, or CoverageJSON, with optional spatial subsetting.

---

## URL structure

```
/coverages/                                                           landing page
/coverages/conformance                                                OGC conformance
/coverages/catalogs/{catalog_id}/collections/{collection_id}/coverage            stream coverage
/coverages/catalogs/{catalog_id}/collections/{collection_id}/coverage/metadata   metadata + links
/coverages/catalogs/{catalog_id}/collections/{collection_id}/coverage/domainset  axes + CRS
/coverages/catalogs/{catalog_id}/collections/{collection_id}/coverage/rangetype  band field defs
```

OGC conformance declared:
```
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/core
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geodata-coverage
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/json
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/html
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-subset
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-bbox
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coverage-datetime
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/geotiff
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/netcdf
http://www.opengis.net/spec/ogcapi-coverages-1/1.0/conf/coveragejson
```

Note: `zarr` is implemented but has no OGC conformance URI declared yet.

---

## Request parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `f` | str | `geotiff` | Output format: `geotiff`, `netcdf`, `zarr`, `covjson` |
| `subset` | str | none | OGC subset grammar: `Lon(10:20),Lat(30:40)` |

`CoveragesConfig` runtime knobs (per-catalog/collection, none currently enforced):

```python
max_bands_per_request: int          # declared; not checked
request_deadline_soft_s: int = 60   # declared; not checked
request_deadline_hard_s: int = 120  # declared; not checked
default_block_size: int = 512       # declared; read_window_iter hardcodes 512
default_style_id: str               # for Styles integration
```

---

## Coverage streaming pipeline

```
GET /coverages/.../coverage?f=netcdf&subset=Lon(10:20),Lat(30:40)
        │
        ├─ 1. _get_first_coverage_item() → CatalogsProtocol.search_items(limit=1)
        ├─ 2. _asset_href(item) → resolve "data" or "coverage" asset key
        ├─ 3. build_rangetype(item) → band names from raster:bands STAC metadata
        ├─ 4. parse_subset(subset) → SubsetRequest [AxisRange list]
        ├─ 5. open_raster_vsi(href) → rasterio DatasetReader via GDAL VSI
        ├─ 6. RasterGeoRef(affine, crs, axis_order)
        ├─ 7. resolve_window(req, ref) → WindowBox(col_off, row_off, w, h)
        ├─ 8. compute output bbox from WindowBox + affine
        ├─ 9. read_window_iter(ds, box, band, block=512) → numpy arrays (chunked)
        └─ 10. writer for format → StreamingResponse
```

The `/coverage/domainset` and `/coverage/rangetype` endpoints are pure
transforms from the STAC item (no pixel I/O):
- `build_domainset(item)` — extracts bbox, `proj:epsg` CRS, datetime axis
- `build_rangetype(item)` — reads `raster:bands`, maps dtype → OGC data-type URI,
  carries `nodata` as `nilValues`

Key files:

| File | Responsibility |
|---|---|
| `extensions/coverages/coverages_service.py` | Router, orchestration (priority 160) |
| `extensions/coverages/coverages_models.py` | `CoveragesLandingPage`, `DomainSet`, `RangeType`, `CoverageDescription` |
| `extensions/coverages/links.py` | `build_coverage_links()` — Styles + Maps link set for metadata |
| `extensions/coverages/config.py` | `CoveragesConfig` runtime knobs |
| `modules/coverages/domainset.py` | STAC item → OGC DomainSet transform |
| `modules/coverages/rangetype.py` | STAC item → OGC RangeType transform |
| `modules/coverages/subset.py` | OGC subset grammar parser → `SubsetRequest` |
| `modules/coverages/window.py` | `SubsetRequest` + affine → `WindowBox` pixel coordinates |
| `modules/coverages/reader.py` | Chunked rasterio band reader (`read_window_iter`) |
| `modules/coverages/writers/geotiff.py` | GeoTIFF writer (rasterio `MemoryFile`) |
| `modules/coverages/writers/netcdf.py` | NetCDF-4 writer (xarray + netCDF4 engine) |
| `modules/coverages/writers/zarr.py` | Zarr writer (xarray + `ZipStore`) |
| `modules/coverages/writers/coveragejson.py` | CoverageJSON writer (placeholder) |

---

## Output formats

| `?f=` | Media type | Writer | Notes |
|---|---|---|---|
| `geotiff` (default) | `image/tiff;application=geotiff` | rasterio `MemoryFile` | Tiled, deflate, 256×256 blocks |
| `netcdf` | `application/x-netcdf` | xarray + netCDF4 | CF-1.8; temp file, streamed 1 MB chunks |
| `zarr` | `application/x-zarr` | xarray + zarr `ZipStore` | CF-1.8; ZIP archive, streamed 1 MB chunks |
| `covjson` | `application/prs.coverage+json` | JSON | **Placeholder** — pixel values not wired yet |

### NetCDF-4 and Zarr details

Both writers share the same xarray construction path:

1. Tile arrays from `read_window_iter` are accumulated per band into a single
   `float32` ndarray of shape `(height, width)`.
2. Coordinate arrays: `np.linspace(bbox[0], bbox[2], width)` for lon,
   `np.linspace(bbox[3], bbox[1], height)` for lat (north at index 0).
3. Each band becomes a `DataArray` with `dims=["lat","lon"]` and
   `grid_mapping="crs"`. A scalar `crs` variable carries the full CRS WKT.
4. Attributes follow CF-1.8 conventions (`Conventions`, `axis`, `standard_name`).

**NetCDF-4:** written via `ds.to_netcdf(tmp_file, engine="netcdf4")`, then streamed
and deleted.

**Zarr:** chunked along lat/lon at `chunk_size` (default 256 px, capped at
dimension size), consolidated metadata, stored as `zarr.storage.ZipStore`.
Result ZIP is streamed then deleted.

---

## Dependencies

| Package | Extra | Purpose |
|---|---|---|
| `rasterio` | `module_gdal` / `geospatial_io` | All raster reads + GeoTIFF write |
| `numpy` | geospatial stack | Array arithmetic |
| `netCDF4>=1.6.0` | `module_coverages_netcdf` | NetCDF-4 engine for xarray |
| `xarray>=2024.0.0` | `module_coverages_netcdf` or `module_coverages_zarr` | Dataset construction |
| `zarr>=2.18.0` | `module_coverages_zarr` | Zarr ZipStore write |
| `lxml` | `geospatial_io` | (indirect; used by styles) |

---

## Known limitations

1. **First-item only** — `_get_first_coverage_item` always fetches `limit=1`
   with no temporal or spatial filter; there is no mechanism to select a
   specific item within a collection.
2. **CoverageJSON is a placeholder** — `write_coveragejson` receives an empty
   iterator; the `ranges` arrays are always empty.
3. **Zarr has no conformance URI** — it is served but not declared in
   `OGC_API_COVERAGES_URIS`.
4. **In-memory accumulation** — NetCDF and Zarr writers accumulate all tiles
   into memory before writing; very large coverages will OOM before streaming.
5. **Config knobs not enforced** — `max_bands_per_request`, deadline fields,
   and `default_block_size` are declared in `CoveragesConfig` but none are
   checked in the handler code.
