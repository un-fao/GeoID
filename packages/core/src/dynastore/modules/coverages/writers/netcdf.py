#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""Streaming NetCDF-4 writer with CF-1.8 conventions.

Uses xarray as the high-level API and netCDF4 as the I/O engine.
Imports are deferred so the module is usable without those packages.
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator, List, Tuple


def write_netcdf(
    *,
    width: int,
    height: int,
    bbox: List[float],
    crs: str,
    band_names: List[str],
    tiles: Iterable[Tuple[int, int, Any]],
    chunk_bytes: int = 1 << 20,
) -> Iterator[bytes]:
    """Yield NetCDF-4 file bytes with CF conventions.

    Tiles are consumed into a full in-memory numpy array before encoding.
    Each tile is ``(col_off, row_off, ndarray)`` where the array is 2-D
    ``(h, w)`` for a single band or 3-D ``(bands, h, w)`` for multi-band.
    Band iteration is driven by the order of entries in ``band_names``.
    """
    import os
    import tempfile

    import numpy as np
    import xarray as xr

    # One float32 accumulation buffer per band
    data: dict[str, np.ndarray] = {
        name: np.full((height, width), fill_value=np.nan, dtype="float32")
        for name in band_names
    }

    band_idx = 0
    for col, row, arr in tiles:
        arr = np.asarray(arr)
        if arr.ndim == 3:
            # Multi-band tile: (num_bands, h, w)
            for b in range(min(arr.shape[0], len(band_names))):
                h, w = arr.shape[1], arr.shape[2]
                data[band_names[b]][row: row + h, col: col + w] = arr[b]
        else:
            # Single-band tile: (h, w)
            h, w = arr.shape[-2], arr.shape[-1]
            name = band_names[band_idx % len(band_names)]
            data[name][row: row + h, col: col + w] = arr
            band_idx += 1

    lons = np.linspace(bbox[0], bbox[2], width, dtype="float64")
    lats = np.linspace(bbox[3], bbox[1], height, dtype="float64")

    data_vars: dict[str, xr.DataArray] = {}
    for name, arr2d in data.items():
        da = xr.DataArray(
            arr2d,
            dims=["lat", "lon"],
            coords={"lat": lats, "lon": lons},
        )
        da.attrs["grid_mapping"] = "crs"
        data_vars[name] = da

    ds = xr.Dataset(data_vars)
    ds.attrs["Conventions"] = "CF-1.8"
    ds["lon"].attrs = {
        "units": "degrees_east",
        "standard_name": "longitude",
        "long_name": "longitude",
        "axis": "X",
    }
    ds["lat"].attrs = {
        "units": "degrees_north",
        "standard_name": "latitude",
        "long_name": "latitude",
        "axis": "Y",
    }
    # Grid-mapping scalar variable stores CRS metadata
    ds["crs"] = xr.DataArray(np.int32(0))
    ds["crs"].attrs["grid_mapping_name"] = "latitude_longitude"
    ds["crs"].attrs["crs_wkt"] = crs

    tf = tempfile.NamedTemporaryFile(prefix="cov-", suffix=".nc", delete=False)
    tf.close()
    try:
        ds.to_netcdf(tf.name, engine="netcdf4", format="NETCDF4")
        with open(tf.name, "rb") as fh:
            while True:
                chunk = fh.read(chunk_bytes)
                if not chunk:
                    break
                yield chunk
    finally:
        os.unlink(tf.name)
