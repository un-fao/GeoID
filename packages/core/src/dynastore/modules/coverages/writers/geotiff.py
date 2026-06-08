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

"""Streaming GeoTIFF writer backed by a rasterio MemoryFile."""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Tuple


def write_geotiff(
    *,
    width: int,
    height: int,
    transform,
    crs,
    dtype: str,
    band_count: int,
    tiles: Iterable[Tuple[int, int, Any]],
) -> Iterator[bytes]:
    import rasterio

    with rasterio.MemoryFile() as mem:
        with mem.open(
            driver="GTiff",
            width=width, height=height, count=band_count,
            dtype=dtype, transform=transform, crs=crs,
            tiled=True, blockxsize=256, blockysize=256, compress="deflate",
        ) as dst:
            for col, row, arr in tiles:
                h, w = arr.shape[-2], arr.shape[-1]
                dst.write(arr, 1, window=rasterio.windows.Window(col, row, w, h))  # type: ignore
        yield mem.read()
