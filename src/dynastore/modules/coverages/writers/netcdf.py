"""Streaming NetCDF writer — temp-file bounded."""

from __future__ import annotations

from typing import Iterable, Iterator, List, Tuple


def write_netcdf(
    *,
    width: int,
    height: int,
    bbox: List[float],
    crs: str,
    band_names: List[str],
    tiles: Iterable[Tuple[int, int, object]],
    chunk_bytes: int = 1 << 20,
) -> Iterator[bytes]:
    import os
    import tempfile

    import numpy as np
    from netCDF4 import Dataset  # type: ignore[import-not-found]

    tf = tempfile.NamedTemporaryFile(prefix="cov-", suffix=".nc", delete=False)
    tf.close()
    try:
        ds = Dataset(tf.name, "w", format="NETCDF4")
        ds.createDimension("lon", width)
        ds.createDimension("lat", height)
        lon = ds.createVariable("lon", "f4", ("lon",))
        lat = ds.createVariable("lat", "f4", ("lat",))
        lon[:] = np.linspace(bbox[0], bbox[2], width)
        lat[:] = np.linspace(bbox[3], bbox[1], height)
        variables = {
            name: ds.createVariable(name, "f4", ("lat", "lon"), zlib=True)
            for name in band_names
        }
        for col, row, arr in tiles:
            h, w = arr.shape[-2], arr.shape[-1]
            name = band_names[0]
            variables[name][row:row + h, col:col + w] = arr
        ds.close()

        with open(tf.name, "rb") as fh:
            while True:
                chunk = fh.read(chunk_bytes)
                if not chunk:
                    break
                yield chunk
    finally:
        os.unlink(tf.name)
