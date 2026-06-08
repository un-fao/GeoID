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
from dynastore.modules.edr.query_handlers.cube import parse_cube_bbox


def test_parse_4_values():
    assert parse_cube_bbox("-10.0,-20.0,30.0,40.0") == (-10.0, -20.0, 30.0, 40.0)


def test_parse_with_spaces():
    assert parse_cube_bbox("0, 0, 1, 1") == (0.0, 0.0, 1.0, 1.0)


def test_parse_6_values_uses_first_4():
    result = parse_cube_bbox("0,0,1,1,100,200")
    assert result == (0.0, 0.0, 1.0, 1.0)


def test_parse_rejects_fewer_than_4():
    with pytest.raises(ValueError, match="bbox must have at least 4"):
        parse_cube_bbox("0,0,1")
