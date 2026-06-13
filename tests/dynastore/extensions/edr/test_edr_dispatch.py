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


def test_format_resolves_default_to_covjson():
    from dynastore.extensions.edr.edr_service import _resolve_edr_format
    assert _resolve_edr_format(None) == "covjson"


def test_format_resolves_covjson_variants():
    from dynastore.extensions.edr.edr_service import _resolve_edr_format
    assert _resolve_edr_format("covjson") == "covjson"
    assert _resolve_edr_format("CoverageJSON") == "covjson"
    assert _resolve_edr_format("COVJSON") == "covjson"


def test_format_resolves_geojson():
    from dynastore.extensions.edr.edr_service import _resolve_edr_format
    assert _resolve_edr_format("geojson") == "geojson"
    assert _resolve_edr_format("GeoJSON") == "geojson"


def test_format_rejects_unknown():
    from fastapi import HTTPException
    from dynastore.extensions.edr.edr_service import _resolve_edr_format
    with pytest.raises(HTTPException) as exc:
        _resolve_edr_format("netcdf")
    assert exc.value.status_code == 415


def test_asset_href_prefers_data_key():
    from dynastore.extensions.ogc_base import ogc_asset_href
    item = {
        "assets": {
            "data": {"href": "gs://bucket/data.tif"},
            "coverage": {"href": "gs://bucket/cov.tif"},
        }
    }
    assert ogc_asset_href(item) == "gs://bucket/data.tif"


def test_asset_href_falls_back_to_any():
    from dynastore.extensions.ogc_base import ogc_asset_href
    item = {"assets": {"thumbnail": {"href": "https://host/thumb.png"}}}
    assert ogc_asset_href(item) == "https://host/thumb.png"


def test_asset_href_raises_404_when_missing():
    from fastapi import HTTPException
    from dynastore.extensions.ogc_base import ogc_asset_href
    with pytest.raises(HTTPException) as exc:
        ogc_asset_href({"assets": {}}, error_detail="No asset href on EDR item.")
    assert exc.value.status_code == 404


def test_edr_conformance_uris_are_set():
    from dynastore.extensions.edr.edr_service import OGC_API_EDR_URIS
    assert any("ogcapi-edr" in uri for uri in OGC_API_EDR_URIS)
    assert len(OGC_API_EDR_URIS) >= 4


def test_resolve_band_names_no_filter():
    from dynastore.extensions.edr.edr_service import _resolve_band_names
    item = {
        "assets": {
            "data": {
                "raster:bands": [
                    {"name": "temperature"},
                    {"name": "humidity"},
                ]
            }
        }
    }
    assert _resolve_band_names(item, None) == ["temperature", "humidity"]


def test_resolve_band_names_with_filter():
    from dynastore.extensions.edr.edr_service import _resolve_band_names
    item = {
        "assets": {
            "data": {
                "raster:bands": [
                    {"name": "temperature"},
                    {"name": "humidity"},
                    {"name": "wind"},
                ]
            }
        }
    }
    assert _resolve_band_names(item, ["temperature", "wind"]) == ["temperature", "wind"]


def test_resolve_band_names_empty_item_returns_value_sentinel():
    from dynastore.extensions.edr.edr_service import _resolve_band_names
    assert _resolve_band_names({}, None) == ["value"]
