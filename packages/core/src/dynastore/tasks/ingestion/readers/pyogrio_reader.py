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

"""Pyogrio-backed reader, registered as a fallback.

Sits strictly behind :class:`GdalOsgeoReader` (``priority=10``), which
binds the **system** libgdal via ``osgeo.ogr``.  This reader hard-imports
``pyogrio`` instead — pyogrio ships its own GDAL build in the PyPI wheel,
so it provides a vector-read path in scopes that pull in ``geospatial_io``
but not the ``module_gdal`` osgeo bindings.  ``priority=100`` keeps it a
tail candidate.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any, ClassVar, Iterable, Iterator, Tuple

# Hard-import gates registration: when geospatial_io isn't installed the
# ImportError prevents the ``register_reader`` call below from running and
# the registry stays narrower (same wrong-SCOPE-soft-skip pattern as the
# rest of the codebase).
import pyogrio  # noqa: F401

from .base import SourceReaderProtocol, _to_vsigs, register_reader

logger = logging.getLogger(__name__)


class PyogrioReader(SourceReaderProtocol):
    """Fallback reader backed by pyogrio's bundled GDAL."""

    reader_id: ClassVar[str] = "pyogrio"
    priority: ClassVar[int] = 100
    extensions: ClassVar[Tuple[str, ...]] = (
        # Keep the set small and overlapping with osgeo intentionally so an
        # explicit ``hint=pyogrio`` (future) can pin it; osgeo's lower
        # priority still wins for these by default.
        ".geojson", ".json", ".gpkg", ".shp", ".csv",
    )

    @contextlib.contextmanager
    def open(
        self,
        uri: str,
        *,
        encoding: str = "utf-8",
        content_type: str | None = None,  # noqa: ARG002 — forwarded by registry, unused here
        **opts: Any,
    ) -> Iterator[Iterable[dict]]:
        import geopandas as gpd

        path = _to_vsigs(uri)
        # Surface the OGR FID under its column name so a collection that
        # declares it as a required/unique field resolves it (#1820 — mirrors
        # GdalOsgeoReader).  ``iterfeatures`` drops the index, so a NAMED fid
        # column (Shapefile's auto-promoted ``FID``, GeoPackage's ``fid``) must
        # be read as the index and reset back into a column; an anonymous
        # record-number FID (``fid_column == ""``) is deliberately left out so
        # plain shapefiles keep their previous attribute shape.
        fid_column = ""
        try:
            fid_column = pyogrio.read_info(path).get("fid_column") or ""
        except Exception:  # noqa: BLE001 — no FID surfacing if introspection fails
            fid_column = ""
        read_kwargs: dict[str, Any] = {"engine": "pyogrio", "encoding": encoding}
        if fid_column:
            read_kwargs["fid_as_index"] = True
        gdf = gpd.read_file(path, **read_kwargs)
        if fid_column and gdf.index.name and fid_column not in gdf.columns:
            idx_name = gdf.index.name
            gdf = gdf.reset_index()
            if idx_name != fid_column and idx_name in gdf.columns:
                gdf = gdf.rename(columns={idx_name: fid_column})
        logger.info("PyogrioReader: opened %r (%d features)", path, len(gdf))
        # ``iterfeatures`` yields GeoJSON-shaped dicts
        # (``{"type": "Feature", "properties": …, "geometry": …}``), matching
        # the record shape the downstream upsert expects.
        yield gdf.iterfeatures()

    def feature_count(
        self, uri: str, *, content_type: str | None = None,  # noqa: ARG002
    ) -> int | None:
        try:
            info = pyogrio.read_info(_to_vsigs(uri))
            count = info.get("features")
            return int(count) if count is not None else None
        except Exception:  # noqa: BLE001
            return None


register_reader(PyogrioReader)
