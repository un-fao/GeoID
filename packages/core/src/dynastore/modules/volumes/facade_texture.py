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

"""Procedurally generate a tileable building-facade texture as PNG bytes.

The open 3D building sources we ingest (3DBAG / 3D Tiles Nederland, CC BY 4.0)
are *geometry only* — they carry no facade photos. Rather than depend on a
proprietary photo-textured tileset, we author the texture ourselves: a simple
window/floor grid. Because it is generated in this module, the texture is our
own Apache-2.0 content and composes with the (separately licensed) geometry
without any licensing friction.

The image is a single repeating cell of windows on a wall background. The glTF
sampler uses REPEAT wrapping, so mapping wall UVs in metres (see
``mesh_builder``) tiles the windows up and along every facade. Two invariants
make it drop-in for the mesh:

* The corner pixel ``(0, 0)`` is always wall colour (windows are inset by a
  margin). Roof/floor caps map their UV to ``(0, 0)`` so they read as flat
  surfaces rather than glass.
* The cell repeats seamlessly, so REPEAT wrapping shows no seam between tiles.

Implementation note: PNG is encoded with the standard library only (``zlib`` +
manual chunk framing with CRC-32). ``packages/core`` must stay free of an image
dependency such as Pillow, so we cannot import PIL/numpy here.
"""

from __future__ import annotations

import functools
import struct
import zlib
from typing import Tuple

RGB = Tuple[int, int, int]

# Default palette — a neutral light wall so a per-building height tint (COLOR_0)
# multiplies through cleanly, with cool glass and a slightly darker frame.
_WALL: RGB = (203, 198, 189)
_FRAME: RGB = (150, 146, 138)
_GLASS_TOP: RGB = (120, 150, 178)     # lighter at the top (faux sky reflection)
_GLASS_BOTTOM: RGB = (64, 84, 108)


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    """Frame one PNG chunk: length + tag + data + CRC-32(tag+data)."""
    body = tag + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)


def encode_png(rgba: bytes, width: int, height: int) -> bytes:
    """Encode a top-to-bottom RGBA byte buffer as an 8-bit PNG.

    *rgba* must be exactly ``width * height * 4`` bytes. Every scanline uses
    filter type 0 (None), so the stream decodes by simply stripping the leading
    filter byte from each row.
    """
    if len(rgba) != width * height * 4:
        raise ValueError(
            f"rgba buffer is {len(rgba)} bytes, expected {width * height * 4}"
        )
    signature = b"\x89PNG\r\n\x1a\n"
    # IHDR: width, height, bit depth 8, colour type 6 (RGBA), default codec bytes.
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 6, 0, 0, 0)

    stride = width * 4
    raw = bytearray()
    for y in range(height):
        raw.append(0)  # filter type 0 (None) for this scanline
        raw += rgba[y * stride:(y + 1) * stride]
    idat = zlib.compress(bytes(raw), 9)

    return (
        signature
        + _png_chunk(b"IHDR", ihdr)
        + _png_chunk(b"IDAT", idat)
        + _png_chunk(b"IEND", b"")
    )


def build_facade_png(
    *,
    size: int = 256,
    cols: int = 4,
    rows: int = 4,
    wall: RGB = _WALL,
    frame: RGB = _FRAME,
    glass_top: RGB = _GLASS_TOP,
    glass_bottom: RGB = _GLASS_BOTTOM,
) -> bytes:
    """Return PNG bytes for a ``size``×``size`` tileable facade.

    The image holds a ``cols``×``rows`` grid of windows on a wall background;
    each window has a thin frame and a vertically graded glass fill. ``size``
    should be divisible by both ``cols`` and ``rows`` so the cell tiles cleanly.
    """
    if size <= 0 or cols <= 0 or rows <= 0:
        raise ValueError("size, cols and rows must be positive")

    cell_w = size // cols
    cell_h = size // rows
    # Window inset within its cell (leaves a wall gutter, so the cell — and the
    # whole texture corner — starts on wall colour).
    mx = max(2, cell_w // 6)
    my = max(2, cell_h // 6)
    fr = 2  # frame thickness in pixels

    win_x0, win_x1 = mx, cell_w - mx
    win_y0, win_y1 = my, cell_h - my
    glass_span = max(1, (win_y1 - fr) - (win_y0 + fr))

    px = bytearray(size * size * 4)
    for y in range(size):
        cy = y % cell_h
        row_base = y * size * 4
        for x in range(size):
            cx = x % cell_w
            i = row_base + x * 4
            if win_x0 <= cx < win_x1 and win_y0 <= cy < win_y1:
                # Inside a window: frame ring, else graded glass.
                if (
                    cx < win_x0 + fr or cx >= win_x1 - fr
                    or cy < win_y0 + fr or cy >= win_y1 - fr
                ):
                    r, g, b = frame
                else:
                    wt = (cy - (win_y0 + fr)) / glass_span
                    r = int(glass_top[0] * (1.0 - wt) + glass_bottom[0] * wt)
                    g = int(glass_top[1] * (1.0 - wt) + glass_bottom[1] * wt)
                    b = int(glass_top[2] * (1.0 - wt) + glass_bottom[2] * wt)
            else:
                r, g, b = wall
            px[i] = r
            px[i + 1] = g
            px[i + 2] = b
            px[i + 3] = 255

    return encode_png(bytes(px), size, size)


@functools.lru_cache(maxsize=1)
def build_default_facade_png() -> bytes:
    """Cached default facade PNG, baked once per process and reused per tile."""
    return build_facade_png()
