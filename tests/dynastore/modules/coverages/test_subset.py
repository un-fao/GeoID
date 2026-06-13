#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

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
