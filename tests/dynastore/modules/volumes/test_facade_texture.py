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

"""Tests for modules/volumes/facade_texture.py.

The PNG is decoded with the standard library only (mirroring how it is encoded)
so the suite carries no image dependency.
"""

from __future__ import annotations

import struct
import zlib

import pytest

from dynastore.modules.volumes.facade_texture import (
    build_default_facade_png,
    build_facade_png,
    encode_png,
)

_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _decode_png(data: bytes):
    """Minimal PNG decoder for filter-0 RGBA images → (w, h, pixels bytes)."""
    assert data[:8] == _SIGNATURE
    pos = 8
    width = height = 0
    idat = bytearray()
    while pos < len(data):
        (length,) = struct.unpack_from(">I", data, pos)
        tag = data[pos + 4:pos + 8]
        chunk = data[pos + 8:pos + 8 + length]
        # Verify the CRC matches (tag + data).
        (crc,) = struct.unpack_from(">I", data, pos + 8 + length)
        assert crc == zlib.crc32(tag + chunk) & 0xFFFFFFFF
        if tag == b"IHDR":
            width, height, bit_depth, color_type = struct.unpack(">IIBB", chunk[:10])
            assert bit_depth == 8
            assert color_type == 6  # RGBA
        elif tag == b"IDAT":
            idat += chunk
        elif tag == b"IEND":
            break
        pos += 12 + length

    raw = zlib.decompress(bytes(idat))
    stride = width * 4
    pixels = bytearray()
    for y in range(height):
        off = y * (stride + 1)
        assert raw[off] == 0  # filter type 0 (None)
        pixels += raw[off + 1:off + 1 + stride]
    return width, height, bytes(pixels)


def _pixel(pixels: bytes, width: int, x: int, y: int):
    i = (y * width + x) * 4
    return tuple(pixels[i:i + 4])


# ---------------------------------------------------------------------------
# encode_png
# ---------------------------------------------------------------------------


def test_encode_png_round_trips_solid_image():
    w, h = 4, 3
    rgba = bytes([10, 20, 30, 255] * (w * h))
    png = encode_png(rgba, w, h)
    dw, dh, pixels = _decode_png(png)
    assert (dw, dh) == (w, h)
    assert pixels == rgba


def test_encode_png_rejects_wrong_buffer_size():
    with pytest.raises(ValueError):
        encode_png(b"\x00\x00\x00", 4, 4)


# ---------------------------------------------------------------------------
# build_facade_png
# ---------------------------------------------------------------------------


def test_facade_png_has_signature_and_dimensions():
    png = build_facade_png(size=64, cols=4, rows=4)
    assert png[:8] == _SIGNATURE
    w, h, _ = _decode_png(png)
    assert (w, h) == (64, 64)


def test_facade_corner_pixel_is_wall_colour():
    # Caps (roofs/floors) map their UV to (0,0); that texel must be wall colour
    # so they render as flat surfaces, not glass.
    png = build_facade_png(size=64, cols=4, rows=4, wall=(203, 198, 189))
    w, _h, pixels = _decode_png(png)
    assert _pixel(pixels, w, 0, 0) == (203, 198, 189, 255)


def test_facade_has_window_pixels_distinct_from_wall():
    wall = (203, 198, 189)
    png = build_facade_png(size=64, cols=4, rows=4, wall=wall)
    w, h, pixels = _decode_png(png)
    # The centre of the first cell falls inside a window → not wall colour.
    cell = 64 // 4
    cx, cy = cell // 2, cell // 2
    assert _pixel(pixels, w, cx, cy)[:3] != wall


def test_facade_is_opaque():
    png = build_facade_png(size=32, cols=2, rows=2)
    w, h, pixels = _decode_png(png)
    alphas = {pixels[(y * w + x) * 4 + 3] for y in range(h) for x in range(w)}
    assert alphas == {255}


def test_facade_is_deterministic():
    assert build_facade_png(size=32) == build_facade_png(size=32)


def test_default_facade_is_cached_and_valid():
    a = build_default_facade_png()
    b = build_default_facade_png()
    assert a is b  # lru_cache returns the same object
    w, h, _ = _decode_png(a)
    assert (w, h) == (256, 256)


def test_facade_rejects_bad_dimensions():
    with pytest.raises(ValueError):
        build_facade_png(size=0)
