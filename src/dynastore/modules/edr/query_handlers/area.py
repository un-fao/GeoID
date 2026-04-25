"""EDR area query handler: extract coverage subset for a WKT POLYGON bbox."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


def parse_wkt_polygon_bbox(wkt: str) -> Tuple[float, float, float, float]:
    """Extract bounding box from WKT POLYGON → (min_lon, min_lat, max_lon, max_lat).

    Pure Python; handles POLYGON and MULTIPOLYGON geometries.
    """
    coord_pairs = re.findall(r"([+-]?\d+(?:\.\d+)?)\s+([+-]?\d+(?:\.\d+)?)", wkt)
    if not coord_pairs:
        raise ValueError(f"Cannot extract coordinates from WKT: {wkt!r}")
    lons = [float(p[0]) for p in coord_pairs]
    lats = [float(p[1]) for p in coord_pairs]
    return min(lons), min(lats), max(lons), max(lats)


def extract_area_values(
    href: str,
    bbox: Tuple[float, float, float, float],
) -> Tuple[Any, Any, Dict[int, List]]:
    """Extract area data from a raster clipped to bbox.

    Returns (RasterGeoRef, WindowBox, {band_index: [[row values], ...]}).
    Lazy-imports rasterio.
    """
    from dynastore.modules.coverages.subset import AxisRange, SubsetRequest
    from dynastore.modules.coverages.window import RasterGeoRef, resolve_window
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.gdal.service import open_raster_vsi

    min_lon, min_lat, max_lon, max_lat = bbox
    req = SubsetRequest(axes=[
        AxisRange("Lon", min_lon, max_lon),
        AxisRange("Lat", min_lat, max_lat),
    ])

    ds = open_raster_vsi(href)
    try:
        t = ds.transform
        ref = RasterGeoRef(
            width=ds.width,
            height=ds.height,
            origin_x=t.c,
            origin_y=t.f,
            pixel_x=t.a,
            pixel_y=t.e,
            crs=str(ds.crs),
            axis_order=("Lon", "Lat"),
        )
        box = resolve_window(req, ref)

        band_arrays: Dict[int, List] = {}
        for band_idx in range(1, ds.count + 1):
            rows: List = []
            for chunk in read_window_iter(ds, box, band=band_idx):
                rows.extend(chunk.tolist())
            band_arrays[band_idx] = rows
        return ref, box, band_arrays
    finally:
        ds.close()
