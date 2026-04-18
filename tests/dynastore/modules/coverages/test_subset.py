import pytest
from dynastore.modules.coverages.subset import (
    SubsetRequest, AxisRange, parse_subset, SubsetParseError,
)


def test_parse_single_axis():
    req = parse_subset("Lat(40:50)")
    assert req == SubsetRequest(axes=[AxisRange("Lat", 40.0, 50.0)])


def test_parse_two_axes():
    req = parse_subset("Lat(40:50),Lon(-10:0)")
    assert req.axes == [
        AxisRange("Lat", 40.0, 50.0),
        AxisRange("Lon", -10.0, 0.0),
    ]


def test_parse_point_axis_low_equals_high():
    req = parse_subset("Time(2024-01-01:2024-01-01)")
    assert req.axes == [AxisRange("Time", "2024-01-01", "2024-01-01")]


def test_parse_none_returns_empty():
    assert parse_subset(None).axes == []
    assert parse_subset("").axes == []


def test_parse_rejects_missing_paren():
    with pytest.raises(SubsetParseError):
        parse_subset("Lat40:50")


def test_parse_rejects_missing_colon():
    with pytest.raises(SubsetParseError):
        parse_subset("Lat(40,50)")


def test_parse_rejects_swapped_bounds():
    with pytest.raises(SubsetParseError):
        parse_subset("Lat(50:40)")
