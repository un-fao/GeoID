import pytest
from dynastore.modules.coverages.writers import writer_for, MEDIA_TYPE_FOR


def test_media_type_map():
    assert MEDIA_TYPE_FOR["geotiff"] == "image/tiff;application=geotiff"
    assert MEDIA_TYPE_FOR["netcdf"] == "application/x-netcdf"
    assert MEDIA_TYPE_FOR["zarr"] == "application/x-zarr"
    assert MEDIA_TYPE_FOR["covjson"] == "application/prs.coverage+json"


def test_writer_for_covjson_is_pure_callable():
    w = writer_for("covjson")
    assert callable(w)


def test_writer_for_netcdf_is_callable():
    w = writer_for("netcdf")
    assert callable(w)


def test_writer_for_zarr_is_callable():
    w = writer_for("zarr")
    assert callable(w)


def test_writer_for_unknown_raises():
    with pytest.raises(ValueError):
        writer_for("bogus")
