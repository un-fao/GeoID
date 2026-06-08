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

import importlib.util
import pytest

_HAS_RASTERIO = importlib.util.find_spec("rasterio") is not None
skipif_no_rasterio = pytest.mark.skipif(not _HAS_RASTERIO, reason="rasterio not installed")


def test_importable_without_rasterio():
    from dynastore.modules.coverages.writers import geotiff
    assert hasattr(geotiff, "write_geotiff")


@skipif_no_rasterio
def test_emits_valid_tiff_bytes():
    import numpy as np
    from rasterio.transform import from_origin
    from dynastore.modules.coverages.writers.geotiff import write_geotiff

    tile = np.zeros((4, 4), dtype="uint8")
    out = b"".join(write_geotiff(
        width=4, height=4, transform=from_origin(0, 4, 1, 1),
        crs="EPSG:4326", dtype="uint8", band_count=1,
        tiles=iter([(0, 0, tile)]),
    ))
    assert out[:4] in (b"II*\x00", b"MM\x00*")
