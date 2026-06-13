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

import json
from dynastore.modules.coverages.writers.coveragejson import write_coveragejson


def test_emits_minimal_grid_coverage():
    domainset = {
        "type": "DomainSet",
        "generalGrid": {
            "srsName": "OGC:CRS84",
            "axisLabels": ["Lon", "Lat"],
            "axis": [
                {"type": "RegularAxis", "axisLabel": "Lon", "lowerBound": 0, "upperBound": 2, "uomLabel": "degree"},
                {"type": "RegularAxis", "axisLabel": "Lat", "lowerBound": 0, "upperBound": 2, "uomLabel": "degree"},
            ],
        },
    }
    rangetype = {"type": "DataRecord", "field": [{"name": "b1", "definition": "float32"}]}
    values_iter = iter([[[1.0, 2.0], [3.0, 4.0]]])

    chunks = list(write_coveragejson(domainset, rangetype, values_iter))
    doc = json.loads(b"".join(chunks).decode())
    assert doc["type"] == "Coverage"
    assert doc["domain"]["axes"]["x"]["start"] == 0
    assert doc["ranges"]["b1"]["values"] == [1.0, 2.0, 3.0, 4.0]


def test_empty_iterator_still_emits_valid_json():
    ds = {"type": "DomainSet", "generalGrid": {"srsName": "OGC:CRS84", "axisLabels": [], "axis": []}}
    rt = {"type": "DataRecord", "field": []}
    chunks = list(write_coveragejson(ds, rt, iter([])))
    doc = json.loads(b"".join(chunks).decode())
    assert doc["type"] == "Coverage"
