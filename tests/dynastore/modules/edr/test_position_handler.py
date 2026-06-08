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
from dynastore.modules.edr.query_handlers.position import parse_wkt_point


def test_parse_wkt_point_basic():
    lon, lat = parse_wkt_point("POINT(12.5 41.9)")
    assert lon == pytest.approx(12.5)
    assert lat == pytest.approx(41.9)


def test_parse_wkt_point_case_insensitive():
    lon, lat = parse_wkt_point("point(0.0 -90.0)")
    assert lon == pytest.approx(0.0)
    assert lat == pytest.approx(-90.0)


def test_parse_wkt_point_negative_lon():
    lon, lat = parse_wkt_point("POINT(-74.006 40.712)")
    assert lon == pytest.approx(-74.006)
    assert lat == pytest.approx(40.712)


def test_parse_wkt_point_with_extra_whitespace():
    lon, lat = parse_wkt_point("  POINT(  1.5   2.5  )  ")
    assert lon == pytest.approx(1.5)
    assert lat == pytest.approx(2.5)


def test_parse_wkt_point_rejects_linestring():
    with pytest.raises(ValueError, match="Invalid WKT POINT"):
        parse_wkt_point("LINESTRING(0 0, 1 1)")


def test_parse_wkt_point_rejects_empty():
    with pytest.raises(ValueError):
        parse_wkt_point("")


def test_parse_wkt_point_rejects_point_without_parens():
    with pytest.raises(ValueError):
        parse_wkt_point("POINT 12 34")
