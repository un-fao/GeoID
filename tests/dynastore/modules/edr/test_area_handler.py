import pytest
from dynastore.modules.edr.query_handlers.area import parse_wkt_polygon_bbox


def test_parse_simple_polygon():
    bbox = parse_wkt_polygon_bbox(
        "POLYGON((10 20, 30 20, 30 40, 10 40, 10 20))"
    )
    assert bbox == (10.0, 20.0, 30.0, 40.0)


def test_parse_polygon_with_negative_coords():
    bbox = parse_wkt_polygon_bbox(
        "POLYGON((-10 -20, 30 -20, 30 40, -10 40, -10 -20))"
    )
    assert bbox == (-10.0, -20.0, 30.0, 40.0)


def test_parse_polygon_global():
    bbox = parse_wkt_polygon_bbox(
        "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))"
    )
    assert bbox == (-180.0, -90.0, 180.0, 90.0)


def test_parse_rejects_empty():
    with pytest.raises(ValueError, match="Cannot extract coordinates"):
        parse_wkt_polygon_bbox("")
