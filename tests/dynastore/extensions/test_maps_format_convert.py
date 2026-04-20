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
def test_geotiff_with_bbox_crs_produces_tiff_bytes():
    out = _convert_png_to_format(
        _TINY_PNG, "geotiff", bbox=[-180.0, -90.0, 180.0, 90.0], crs="EPSG:4326",
    )
    # TIFF little-endian or big-endian magic.
    assert out[:4] in (b"II*\x00", b"MM\x00*")
