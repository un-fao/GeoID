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

_ITEM = {
    "id": "it", "bbox": [0, 0, 1, 1],
    "properties": {"proj:epsg": 4326},
    "assets": {"data": {"raster:bands": [{"name": "b1", "data_type": "uint8"}]}},
}


def test_domainset_helper_returns_generalgrid():
    from dynastore.extensions.coverages.coverages_service import _extract_domainset
    ds = _extract_domainset(_ITEM)
    assert ds["type"] == "DomainSet"


def test_rangetype_helper_returns_datarecord():
    from dynastore.extensions.coverages.coverages_service import _extract_rangetype
    rt = _extract_rangetype(_ITEM)
    assert rt["type"] == "DataRecord"
    assert rt["field"][0]["name"] == "b1"
