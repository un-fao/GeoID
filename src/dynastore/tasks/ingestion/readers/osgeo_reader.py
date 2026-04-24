#    Copyright 2025 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO

"""GdalOsgeoReader — primary reader, uses system libgdal via ``osgeo.ogr``.

Why this exists: ``fiona`` (PyPI wheel) ships a bundled libgdal that
omits the Arrow/Parquet driver.  ``from osgeo import ogr`` binds to
the **system** libgdal (the one that comes with the
``ghcr.io/osgeo/gdal:ubuntu-full-3.12.3`` base image), which DOES
include Parquet, FlatGeobuf, OpenFileGDB, …  Same osgeo binding the
maps service uses successfully.

Hard-imports ``osgeo`` at module load — when SCOPE excludes
``module_gdal`` the import fails and :class:`ReaderRegistry` skips
this reader (same wrong-SCOPE-soft-skip pattern as the rest of the
codebase).
"""

from __future__ import annotations

import contextlib
import json
import logging
from typing import Any, ClassVar, Iterable, Iterator, Tuple

# Hard-import gates registration.  When module_gdal isn't installed
# (most worker scopes), the ImportError prevents this module's
# ``register_reader(GdalOsgeoReader)`` line from running — the registry
# stays narrower and resolve() falls through to the next candidate
# (FionaReader / future PyArrow reader).
from osgeo import ogr, gdal  # noqa: F401

from .base import SourceReaderProtocol, _to_vsigs, register_reader

logger = logging.getLogger(__name__)


# Initialize once.  Idempotent — safe to call repeatedly.
ogr.UseExceptions()
gdal.UseExceptions()


class GdalOsgeoReader(SourceReaderProtocol):
    """Universal reader backed by system libgdal.

    Handles every vector format the base image's GDAL build supports
    (~78 drivers in ubuntu-full): Parquet, FlatGeobuf, GeoJSON,
    GeoPackage, ESRI Shapefile (incl. zipped via ``/vsizip/``), CSV,
    KML, GML, MapInfo, OpenFileGDB …
    """

    reader_id: ClassVar[str] = "gdal_osgeo"
    priority: ClassVar[int] = 10

    # Empty extension tuple means "match anything" — see can_read override.
    extensions: ClassVar[Tuple[str, ...]] = ()

    # Drivers we explicitly know GDAL can open from /vsigs/ + a small
    # extension hint set so the registry's *priority* ordering still
    # picks us over the legacy fiona reader for common formats.
    KNOWN_EXT: ClassVar[Tuple[str, ...]] = (
        ".parquet", ".geoparquet",
        ".fgb",
        ".geojson", ".json",
        ".gpkg",
        ".shp", ".zip",
        ".csv", ".tsv",
        ".kml", ".kmz",
        ".gml",
        ".tab", ".mif",
        ".gdb",
    )

    @classmethod
    def can_read(cls, uri: str) -> bool:
        u = uri.lower()
        return any(u.endswith(ext) for ext in cls.KNOWN_EXT)

    # ------------------------------------------------------------------
    # URI prep
    # ------------------------------------------------------------------

    @staticmethod
    def _to_gdal_uri(uri: str) -> str:
        """Normalize the URI for GDAL.  Translates ``gs://`` and wraps
        zipped shapefiles with ``/vsizip/`` when needed."""
        out = _to_vsigs(uri)
        if out.lower().endswith(".zip"):
            # ESRI shapefile (or other) shipped as a zip.  GDAL needs
            # ``/vsizip//vsigs/<bucket>/<key>.zip`` to descend into it.
            out = "/vsizip/" + out
        return out

    # ------------------------------------------------------------------
    # Open / iterate
    # ------------------------------------------------------------------

    def feature_count(self, uri: str) -> int | None:
        path = self._to_gdal_uri(uri)
        ds = ogr.Open(path)
        if ds is None:
            return None
        try:
            total = 0
            for i in range(ds.GetLayerCount()):
                layer = ds.GetLayer(i)
                if layer is not None:
                    total += layer.GetFeatureCount()
            return total
        finally:
            ds = None  # noqa: F841 — release

    @contextlib.contextmanager
    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        path = self._to_gdal_uri(uri)
        # OGR doesn't honour `encoding=` directly — set via config option.
        # Most modern drivers (Parquet, FGB, GeoJSON, GPKG) are UTF-8 by
        # spec; this only matters for shapefile dbf / CSV.
        prev_enc = gdal.GetConfigOption("SHAPE_ENCODING")
        gdal.SetConfigOption("SHAPE_ENCODING", encoding.upper())
        ds = ogr.Open(path)
        if ds is None:
            raise RuntimeError(
                f"GdalOsgeoReader: ogr.Open({path!r}) returned None — "
                f"no driver matched OR auth/access failed.  "
                f"Drivers available: {[gdal.GetDriver(i).ShortName for i in range(min(gdal.GetDriverCount(), 5))]}…"
            )
        try:
            logger.info(
                "GdalOsgeoReader: opened %r via driver=%s, layers=%d",
                path, ds.GetDriver().GetName(), ds.GetLayerCount(),
            )
            yield self._iter_features(ds)
        finally:
            ds = None  # noqa: F841 — release the dataset / file handle
            if prev_enc is None:
                gdal.SetConfigOption("SHAPE_ENCODING", "")
            else:
                gdal.SetConfigOption("SHAPE_ENCODING", prev_enc)

    @staticmethod
    def _iter_features(ds: Any) -> Iterator[dict]:
        for li in range(ds.GetLayerCount()):
            layer = ds.GetLayer(li)
            if layer is None:
                continue
            layer.ResetReading()
            field_names = [
                layer.GetLayerDefn().GetFieldDefn(i).GetName()
                for i in range(layer.GetLayerDefn().GetFieldCount())
            ]
            for feat in layer:
                if feat is None:
                    continue
                props: dict = {}
                for fname in field_names:
                    try:
                        props[fname] = feat.GetField(fname)
                    except Exception:  # noqa: BLE001
                        props[fname] = None
                geom = feat.GetGeometryRef()
                geom_geojson = None
                geom_wkb = None
                if geom is not None:
                    try:
                        geom_geojson = json.loads(geom.ExportToJson())
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        geom_wkb = bytes(geom.ExportToWkb())
                    except Exception:  # noqa: BLE001
                        pass
                # Mirror fiona's record shape (`{"properties": …,
                # "geometry": …}`) so call sites that already deconstruct
                # fiona records don't need to branch.  Add ``geometry_wkb``
                # as a convenience so column_mapping=geometry_wkb just
                # works for STAC items.
                yield {
                    "type": "Feature",
                    "properties": props,
                    "geometry": geom_geojson,
                    "geometry_wkb": geom_wkb,
                }


register_reader(GdalOsgeoReader)
