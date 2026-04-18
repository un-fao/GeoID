import pytest

from dynastore.modules.coverages.subset import SubsetRequest, AxisRange
from dynastore.modules.coverages.window import (
    RasterGeoRef, WindowBox, WindowResolveError, resolve_window,
)


_GEOREF = RasterGeoRef(
    width=10, height=10,
    origin_x=-10.0, origin_y=10.0,
    pixel_x=2.0, pixel_y=-2.0,
    crs="EPSG:4326",
    axis_order=("Lon", "Lat"),
)


def test_resolve_full_extent_when_empty():
    box = resolve_window(SubsetRequest(), _GEOREF)
    assert box == WindowBox(col_off=0, row_off=0, width=10, height=10)


def test_resolve_lat_lon_subset():
    req = SubsetRequest(axes=[AxisRange("Lat", 0.0, 10.0), AxisRange("Lon", -10.0, 0.0)])
    box = resolve_window(req, _GEOREF)
    assert box == WindowBox(col_off=0, row_off=0, width=5, height=5)


def test_resolve_clamps_out_of_bounds():
    req = SubsetRequest(axes=[AxisRange("Lat", -50.0, 50.0), AxisRange("Lon", -50.0, 50.0)])
    assert resolve_window(req, _GEOREF) == WindowBox(0, 0, 10, 10)


def test_resolve_rejects_unknown_axis():
    req = SubsetRequest(axes=[AxisRange("Bogus", 0.0, 1.0)])
    with pytest.raises(WindowResolveError):
        resolve_window(req, _GEOREF)
