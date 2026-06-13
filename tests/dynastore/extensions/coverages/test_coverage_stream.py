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


def test_helper_importable_without_rasterio():
    from dynastore.extensions.coverages.coverages_service import _stream_coverage_geotiff
    assert callable(_stream_coverage_geotiff)


@skipif_no_rasterio
def test_stream_pipeline_assembles_from_fake_dataset(tmp_path):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin
    from dynastore.extensions.coverages.coverages_service import _stream_coverage_geotiff

    path = tmp_path / "a.tif"
    with rasterio.open(
        path, "w", driver="GTiff", width=8, height=8, count=1,
        dtype="uint8", transform=from_origin(0, 8, 1, 1),
    ) as dst:
        dst.write(np.ones((8, 8), dtype="uint8"), 1)

    out = b"".join(_stream_coverage_geotiff(str(path), subset=None))
    assert out[:4] in (b"II*\x00", b"MM\x00*")
