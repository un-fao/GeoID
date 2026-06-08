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

"""OGC API - Coverages subset parameter parser.

Grammar (OGC 19-087r6 §7.7):
    subset = axis-expr ("," axis-expr)*
    axis-expr = AxisName "(" low ":" high ")"

Pure math — no rasterio import. Output is a typed request object; the
caller resolves it to a concrete rasterio.Window with the dataset's
affine transform in ``window.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Union


class SubsetParseError(ValueError):
    """Raised when the subset string cannot be parsed."""


Coord = Union[float, str]  # numeric for spatial, ISO-8601 string for time


@dataclass(frozen=True)
class AxisRange:
    axis: str
    low: Coord
    high: Coord


@dataclass
class SubsetRequest:
    axes: List[AxisRange] = field(default_factory=list)


_AXIS_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_]*)\(([^:)]+):([^)]+)\)$")


def parse_subset(value: Optional[str]) -> SubsetRequest:
    if not value:
        return SubsetRequest()
    axes: List[AxisRange] = []
    for token in _split_top_level(value):
        m = _AXIS_RE.match(token.strip())
        if not m:
            raise SubsetParseError(f"Invalid axis expression: {token!r}")
        axis, low_s, high_s = m.group(1), m.group(2), m.group(3)
        low, high = _coerce(low_s), _coerce(high_s)
        if isinstance(low, float) and isinstance(high, float) and low > high:
            raise SubsetParseError(
                f"Axis {axis!r}: low ({low}) > high ({high})",
            )
        axes.append(AxisRange(axis, low, high))
    return SubsetRequest(axes=axes)


def _split_top_level(s: str) -> List[str]:
    out, depth, buf = [], 0, []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        out.append("".join(buf))
    return out


def _coerce(raw: str) -> Coord:
    try:
        return float(raw)
    except ValueError:
        return raw
