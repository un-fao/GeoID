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

"""Unit tests for modules/styles/encodings.py — content negotiation helpers."""

import pytest

from dynastore.modules.styles.encodings import (
    MEDIA_TYPE_MAPBOX_GL,
    MEDIA_TYPE_SLD_10,
    MEDIA_TYPE_SLD_11,
    STYLE_FORMAT_TO_MEDIA_TYPE,
    f_param_to_media_type,
    normalize_accept_to_media_type,
)


# ---------------------------------------------------------------------------
# f_param_to_media_type
# ---------------------------------------------------------------------------


def test_f_param_mapbox():
    assert f_param_to_media_type("mapbox") == MEDIA_TYPE_MAPBOX_GL


def test_f_param_json_alias():
    assert f_param_to_media_type("json") == MEDIA_TYPE_MAPBOX_GL


def test_f_param_sld11():
    assert f_param_to_media_type("sld11") == MEDIA_TYPE_SLD_11


def test_f_param_sld10():
    assert f_param_to_media_type("sld10") == MEDIA_TYPE_SLD_10


def test_f_param_unknown():
    assert f_param_to_media_type("unknown") is None


def test_f_param_none():
    assert f_param_to_media_type(None) is None


def test_f_param_case_insensitive():
    assert f_param_to_media_type("MAPBOX") == MEDIA_TYPE_MAPBOX_GL
    assert f_param_to_media_type("SLD11") == MEDIA_TYPE_SLD_11


# ---------------------------------------------------------------------------
# normalize_accept_to_media_type
# ---------------------------------------------------------------------------


def test_accept_empty_returns_first_available():
    available = [MEDIA_TYPE_SLD_11, MEDIA_TYPE_MAPBOX_GL]
    assert normalize_accept_to_media_type("", available) == MEDIA_TYPE_SLD_11


def test_accept_wildcard_returns_first_available():
    available = [MEDIA_TYPE_MAPBOX_GL, MEDIA_TYPE_SLD_11]
    assert normalize_accept_to_media_type("*/*", available) == MEDIA_TYPE_MAPBOX_GL


def test_accept_exact_match():
    available = [MEDIA_TYPE_SLD_11, MEDIA_TYPE_MAPBOX_GL]
    assert (
        normalize_accept_to_media_type(MEDIA_TYPE_SLD_11, available) == MEDIA_TYPE_SLD_11
    )


def test_accept_json_matches_mapbox():
    available = [MEDIA_TYPE_MAPBOX_GL]
    assert normalize_accept_to_media_type("application/json", available) == MEDIA_TYPE_MAPBOX_GL


def test_accept_no_match_returns_none():
    available = [MEDIA_TYPE_MAPBOX_GL]
    assert normalize_accept_to_media_type("text/csv", available) is None


def test_accept_no_available_returns_none():
    assert normalize_accept_to_media_type("application/json", []) is None


def test_accept_quality_ordering():
    # sld11 with q=0.5, mapbox with q=1.0 → mapbox wins
    available = [MEDIA_TYPE_SLD_11, MEDIA_TYPE_MAPBOX_GL]
    accept = f"{MEDIA_TYPE_SLD_11};q=0.5, {MEDIA_TYPE_MAPBOX_GL};q=1.0"
    assert normalize_accept_to_media_type(accept, available) == MEDIA_TYPE_MAPBOX_GL


# ---------------------------------------------------------------------------
# STYLE_FORMAT_TO_MEDIA_TYPE constants
# ---------------------------------------------------------------------------


def test_format_map_covers_enum_values():
    assert "MapboxGL" in STYLE_FORMAT_TO_MEDIA_TYPE
    assert "SLD_1.1" in STYLE_FORMAT_TO_MEDIA_TYPE
