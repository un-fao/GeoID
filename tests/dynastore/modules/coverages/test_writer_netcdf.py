import importlib.util
import pytest

_HAS_NETCDF4 = importlib.util.find_spec("netCDF4") is not None
_HAS_XARRAY = importlib.util.find_spec("xarray") is not None
skipif_no_netcdf4 = pytest.mark.skipif(
    not (_HAS_NETCDF4 and _HAS_XARRAY), reason="netCDF4 and xarray not installed"
)


def test_importable_without_netcdf4():
    from dynastore.modules.coverages.writers import netcdf
    assert hasattr(netcdf, "write_netcdf")


@skipif_no_netcdf4
def test_emits_valid_netcdf_magic():
    import numpy as np
    from dynastore.modules.coverages.writers.netcdf import write_netcdf

    arr = np.zeros((4, 4), dtype="float32")
    out = b"".join(write_netcdf(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["b1"], tiles=iter([(0, 0, arr)]),
    ))
    # HDF5 (NetCDF-4) magic
    assert out[:4] in (b"CDF\x01", b"CDF\x02", b"\x89HDF")


@skipif_no_netcdf4
def test_cf_conventions_attribute():
    import io
    import numpy as np
    import xarray as xr
    from dynastore.modules.coverages.writers.netcdf import write_netcdf

    arr = np.ones((4, 4), dtype="float32")
    out = b"".join(write_netcdf(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["temperature"], tiles=iter([(0, 0, arr)]),
    ))
    ds = xr.open_dataset(io.BytesIO(out), engine="scipy")
    assert ds.attrs.get("Conventions") == "CF-1.8"
    assert "temperature" in ds.data_vars
    assert "crs" in ds


@skipif_no_netcdf4
def test_multiband_tiles():
    import numpy as np
    from dynastore.modules.coverages.writers.netcdf import write_netcdf

    arr0 = np.full((4, 4), 1.0, dtype="float32")
    arr1 = np.full((4, 4), 2.0, dtype="float32")
    out = b"".join(write_netcdf(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["red", "green"],
        tiles=iter([(0, 0, arr0), (0, 0, arr1)]),
    ))
    assert out[:4] in (b"CDF\x01", b"CDF\x02", b"\x89HDF")


@skipif_no_netcdf4
def test_3d_tile_multiband():
    import numpy as np
    from dynastore.modules.coverages.writers.netcdf import write_netcdf

    arr = np.stack([
        np.full((4, 4), 10.0, dtype="float32"),
        np.full((4, 4), 20.0, dtype="float32"),
    ])  # shape (2, 4, 4)
    out = b"".join(write_netcdf(
        width=4, height=4, bbox=[0.0, 0.0, 4.0, 4.0], crs="EPSG:4326",
        band_names=["band1", "band2"],
        tiles=iter([(0, 0, arr)]),
    ))
    assert out[:4] in (b"CDF\x01", b"CDF\x02", b"\x89HDF")
