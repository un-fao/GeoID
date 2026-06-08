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
from dynastore.modules.edr.parameter_metadata import (
    _select_bands,
    build_parameters,
    filter_parameters,
)


_ITEM_WITH_BANDS = {
    "assets": {
        "data": {
            "href": "gs://bucket/file.tif",
            "raster:bands": [
                {"name": "temperature", "unit": "K", "description": "Air temperature"},
                {"name": "humidity", "unit": "%", "description": "Relative humidity"},
            ],
        }
    }
}


def test_select_bands_prefers_data_key():
    bands = _select_bands(_ITEM_WITH_BANDS)
    assert len(bands) == 2
    assert bands[0]["name"] == "temperature"


def test_select_bands_returns_empty_for_no_assets():
    assert _select_bands({}) == []
    assert _select_bands({"assets": {}}) == []


def test_build_parameters_returns_dict_keyed_by_name():
    params = build_parameters(_ITEM_WITH_BANDS)
    assert "temperature" in params
    assert "humidity" in params
    assert params["temperature"]["type"] == "Parameter"
    assert params["temperature"]["unit"]["label"]["en"] == "K"


def test_build_parameters_empty_item():
    assert build_parameters({}) == {}


def test_filter_parameters_keeps_matching():
    bands = [{"name": "temperature"}, {"name": "humidity"}, {"name": "wind"}]
    filtered = filter_parameters(bands, ["temperature", "wind"])
    assert len(filtered) == 2
    assert filtered[0]["name"] == "temperature"
    assert filtered[1]["name"] == "wind"


def test_filter_parameters_none_returns_all():
    bands = [{"name": "temperature"}, {"name": "humidity"}]
    assert filter_parameters(bands, None) == bands
