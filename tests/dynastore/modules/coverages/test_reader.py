import importlib.util
import pytest

_HAS_RASTERIO = importlib.util.find_spec("rasterio") is not None
skipif_no_rasterio = pytest.mark.skipif(not _HAS_RASTERIO, reason="rasterio not installed")


def test_module_importable_without_rasterio():
    from dynastore.modules.coverages import reader
    assert hasattr(reader, "read_window_iter")


@skipif_no_rasterio
def test_read_window_iter_emits_block_tiles(tmp_path):
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin
    from dynastore.modules.coverages.reader import read_window_iter
    from dynastore.modules.coverages.window import WindowBox

    path = tmp_path / "tiny.tif"
    data = np.arange(64, dtype="uint8").reshape(8, 8)
    with rasterio.open(
        path, "w", driver="GTiff", height=8, width=8, count=1,
        dtype="uint8", transform=from_origin(0, 8, 1, 1),
        tiled=True, blockxsize=4, blockysize=4,
    ) as dst:
        dst.write(data, 1)

    with rasterio.open(path) as ds:
        tiles = list(read_window_iter(ds, WindowBox(0, 0, 8, 8), band=1, block=4))
    assert len(tiles) == 4
    assert all(t.shape == (4, 4) for t in tiles)
