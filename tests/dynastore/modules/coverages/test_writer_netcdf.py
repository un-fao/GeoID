import importlib.util
import pytest

_HAS_NETCDF4 = importlib.util.find_spec("netCDF4") is not None
skipif_no_netcdf4 = pytest.mark.skipif(not _HAS_NETCDF4, reason="netCDF4 not installed")


def test_importable_without_netcdf4():
    from dynastore.modules.coverages.writers import netcdf
    assert hasattr(netcdf, "write_netcdf")


@skipif_no_netcdf4
def test_emits_valid_netcdf_magic():
    import numpy as np
    from dynastore.modules.coverages.writers.netcdf import write_netcdf
    arr = np.zeros((4, 4), dtype="float32")
    out = b"".join(write_netcdf(
        width=4, height=4, bbox=[0, 0, 4, 4], crs="EPSG:4326",
        band_names=["b1"], tiles=iter([(0, 0, arr)]),
    ))
    assert out[:4] in (b"CDF\x01", b"CDF\x02", b"\x89HDF")
