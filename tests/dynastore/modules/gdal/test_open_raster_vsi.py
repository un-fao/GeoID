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


def test_signature_exists_without_rasterio():
    from dynastore.modules.gdal.service import open_raster_vsi
    assert callable(open_raster_vsi)


def test_vsi_path_translation_is_pure():
    from dynastore.modules.gdal.service import _href_to_vsi_path
    assert _href_to_vsi_path("gs://bucket/obj.tif") == "/vsigs/bucket/obj.tif"
    assert _href_to_vsi_path("s3://b/o.tif") == "/vsis3/b/o.tif"
    assert _href_to_vsi_path("https://ex.com/a.tif") == "/vsicurl/https://ex.com/a.tif"
    assert _href_to_vsi_path("/local/a.tif") == "/local/a.tif"
