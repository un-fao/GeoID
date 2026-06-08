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

from dynastore.modules.coverages.rangetype import build_rangetype


_ITEM_WITH_BANDS = {
    "assets": {
        "data": {
            "raster:bands": [
                {"name": "red",   "data_type": "uint16", "unit": "dn", "nodata": 0},
                {"name": "green", "data_type": "uint16", "unit": "dn", "nodata": 0},
            ]
        }
    }
}


def test_builds_two_field_record():
    rt = build_rangetype(_ITEM_WITH_BANDS)
    assert rt["type"] == "DataRecord"
    names = [f["name"] for f in rt["field"]]
    assert names == ["red", "green"]
    assert rt["field"][0]["definition"].endswith("#uint16") or "unsignedShort" in rt["field"][0]["definition"]
    assert rt["field"][0]["nilValues"][0]["value"] == "0"
    assert rt["field"][0]["uom"]["code"] == "dn"


def test_empty_when_no_bands():
    assert build_rangetype({"assets": {}}) == {"type": "DataRecord", "field": []}


def test_none_item_returns_none():
    assert build_rangetype(None) is None
