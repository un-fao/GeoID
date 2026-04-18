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
