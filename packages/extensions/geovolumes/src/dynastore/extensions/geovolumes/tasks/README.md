# GeoVolumes Task — 3D Tiles 1.1 Generation

The `geovolumes_tileset` OGC Process builds a 3D Tiles 1.1 tileset from
CityJSON features stored in a DynaStore collection.

## Runtime approach (pure Python)

Dependencies: `trimesh >= 4`, `mapbox_earcut`.

Each CityJSONFeature stored in the `cityjson` extras field is:

1. Dequantized from its integer representation using the collection's
   `cityjson:transform` (scale + translate).
2. Projected from the source CRS (stored as `cityjson:referenceSystem` /
   EPSG) to ECEF (EPSG:4978) via pyproj.
3. Triangulated surface-by-surface with mapbox_earcut.
4. Assembled into a trimesh Scene and exported as a GLB binary.

Up to `MAX_FEATURES_PER_TILE` (50 000) features land in one GLB tile. Larger
collections are partitioned into multiple tiles using a naive linear split;
each tile becomes a child of the root tile in `tileset.json`.

## Offline alternatives for large cities (> 100 MB)

For production-scale datasets where the pure-Python path is too slow or memory-
intensive, two well-tested offline tools can produce equivalent output:

### tyler 0.4.1

<https://github.com/nicholasgasior/tyler>

tyler is a Go CLI that reads CityJSON or CityGML files and writes a 3D Tiles
1.1 tileset directly to disk.  It is not a runtime dependency — run it as a
one-shot pre-processing step and then upload the resulting directory to GCS
using `gsutil rsync`.

```bash
tyler --input city.jsonl --output ./tiles --max-features-per-tile 5000
gsutil -m rsync -r ./tiles gs://bucket/3dtiles/my-collection/
```

### pg2b3dm

<https://github.com/Geodan/pg2b3dm>

pg2b3dm reads 3D geometry from a PostGIS table and writes Batched 3D Model
(b3dm) or GLB tiles compatible with 3D Tiles 1.0/1.1.

```bash
pg2b3dm -h localhost -U postgres -d mydb \
        -t public.buildings -g geometry \
        --output ./tiles
```

Neither tyler nor pg2b3dm is imported or invoked at runtime by this module.
