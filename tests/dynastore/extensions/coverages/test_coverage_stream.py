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
