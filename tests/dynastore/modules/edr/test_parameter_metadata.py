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
