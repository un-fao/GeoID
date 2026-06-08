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

from dynastore.modules.coverages.domainset import build_domainset


_STAC_ITEM = {
    "id": "ndvi-2024-01",
    "bbox": [-10.0, 40.0, 0.0, 50.0],
    "properties": {
        "proj:epsg": 4326,
        "proj:transform": [0.01, 0, -10.0, 0, -0.01, 50.0],
        "proj:shape": [1000, 1000],
    },
}


def test_builds_geo_axes_from_proj_fields():
    ds = build_domainset(_STAC_ITEM)
    assert ds["type"] == "DomainSet"
    axes = {a["axisLabel"]: a for a in ds["generalGrid"]["axis"]}
    assert "Lon" in axes and "Lat" in axes
    assert axes["Lon"]["lowerBound"] == -10.0
    assert axes["Lon"]["upperBound"] == 0.0
    assert axes["Lat"]["lowerBound"] == 40.0
    assert axes["Lat"]["upperBound"] == 50.0


def test_falls_back_to_bbox_when_proj_missing():
    item = {"id": "x", "bbox": [0, 0, 1, 1], "properties": {}}
    ds = build_domainset(item)
    axes = {a["axisLabel"]: a for a in ds["generalGrid"]["axis"]}
    assert axes["Lon"]["lowerBound"] == 0 and axes["Lon"]["upperBound"] == 1


def test_adds_time_axis_when_datetime_present():
    item = {
        **_STAC_ITEM,
        "properties": {**_STAC_ITEM["properties"], "datetime": "2024-01-15T00:00:00Z"},
    }
    ds = build_domainset(item)
    axes = {a["axisLabel"]: a for a in ds["generalGrid"]["axis"]}
    assert "Time" in axes
    assert axes["Time"]["lowerBound"] == "2024-01-15T00:00:00Z"


def test_returns_none_when_item_missing():
    assert build_domainset(None) is None
