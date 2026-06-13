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

"""Unit tests for Maps output-format conversion helper.

Guards the JPEG + GeoTIFF branches added in Pass 1. The GeoTIFF happy
path needs rasterio (shipped from the GDAL base image in production,
absent in local dev venvs) and is skipped when unavailable.
"""

from __future__ import annotations

import importlib.util

import pytest
from fastapi import HTTPException

from dynastore.extensions.maps.format_convert import (
    convert_png_to_format as _convert_png_to_format,
)


# A minimal 1x1 transparent PNG.
_TINY_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00"
    b"\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
)


def test_png_passthrough_returns_input_bytes():
    out = _convert_png_to_format(_TINY_PNG, "png")
    assert out is _TINY_PNG


@pytest.mark.skipif(
    importlib.util.find_spec("PIL") is None,
    reason="Pillow not installed",
)
def test_jpeg_conversion_returns_jpeg_bytes():
    out = _convert_png_to_format(_TINY_PNG, "jpeg")
    # JPEG SOI marker.
    assert out.startswith(b"\xff\xd8\xff")
    assert out != _TINY_PNG


def test_geotiff_requires_bbox_and_crs():
    with pytest.raises(HTTPException) as exc:
        _convert_png_to_format(_TINY_PNG, "geotiff")
    assert exc.value.status_code == 400


def test_unsupported_format_raises_415():
    with pytest.raises(HTTPException) as exc:
        _convert_png_to_format(_TINY_PNG, "webp")
    assert exc.value.status_code == 415


@pytest.mark.skipif(
    importlib.util.find_spec("rasterio") is None,
    reason="rasterio not installed in local dev venv",
)
@pytest.mark.xfail(reason="#514 — PIL not installed in unit-test SCOPE; either add to deps or move under integration.", strict=False)
def test_geotiff_with_bbox_crs_produces_tiff_bytes():
    out = _convert_png_to_format(
        _TINY_PNG, "geotiff", bbox=[-180.0, -90.0, 180.0, 90.0], crs="EPSG:4326",
    )
    # TIFF little-endian or big-endian magic.
    assert out[:4] in (b"II*\x00", b"MM\x00*")
