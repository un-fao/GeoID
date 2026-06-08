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

def test_metadata_response_shape():
    from dynastore.extensions.coverages.coverages_service import (
        _build_metadata_response,
    )
    item = {
        "id": "it",
        "bbox": [-10, 40, 0, 50],
        "properties": {"proj:epsg": 4326, "datetime": "2024-01-15T00:00:00Z"},
        "assets": {"data": {"raster:bands": [
            {"name": "b1", "data_type": "uint16", "nodata": 0},
        ]}},
    }
    resp = _build_metadata_response(
        item=item,
        base_url="http://ex",
        catalog_id="cat",
        collection_id="col",
        default_style_id="ndvi",
    )
    assert resp["title"] == "it"
    assert resp["domainset"]["type"] == "DomainSet"
    assert resp["rangetype"]["field"][0]["name"] == "b1"
    rels = {lk["rel"] for lk in resp["links"]}
    assert "self" in rels and "data" in rels and "styles" in rels
