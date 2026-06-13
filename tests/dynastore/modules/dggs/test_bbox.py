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

"""Unit tests for the shared modules.dggs.bbox.parse_bbox helper."""

import pytest

from dynastore.modules.dggs.bbox import parse_bbox


def test_parse_bbox_valid():
    result = parse_bbox("10.0,20.0,30.0,40.0")
    assert result == (10.0, 20.0, 30.0, 40.0)


def test_parse_bbox_none():
    assert parse_bbox(None) is None
    assert parse_bbox("") is None


def test_parse_bbox_invalid_count():
    with pytest.raises(ValueError, match="4"):
        parse_bbox("10,20,30")


def test_parse_bbox_non_numeric():
    with pytest.raises(ValueError):
        parse_bbox("a,b,c,d")


def test_parse_bbox_degenerate():
    with pytest.raises(ValueError, match="degenerate"):
        parse_bbox("10,20,5,40")  # xmin > xmax
