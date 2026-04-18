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
