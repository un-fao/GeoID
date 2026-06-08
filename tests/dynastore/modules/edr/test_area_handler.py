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


def test_parse_3d_polygon_ignores_z():
    bbox = parse_wkt_polygon_bbox(
        "POLYGON((10 20 100, 30 20 100, 30 40 100, 10 40 100, 10 20 100))"
    )
    assert bbox == (10.0, 20.0, 30.0, 40.0)


def test_parse_rejects_empty():
    with pytest.raises(ValueError, match="Cannot extract coordinates"):
        parse_wkt_polygon_bbox("")
