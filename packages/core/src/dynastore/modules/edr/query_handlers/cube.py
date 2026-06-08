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

"""EDR cube query handler: parse bbox and delegate to area extraction."""

from __future__ import annotations

from typing import Tuple


def parse_cube_bbox(bbox_str: str) -> Tuple[float, float, float, float]:
    """Parse EDR cube bbox param → (min_lon, min_lat, max_lon, max_lat).

    Accepts comma-separated values; optional 3D/4D extra values are ignored.
    """
    parts = [p.strip() for p in bbox_str.split(",")]
    if len(parts) < 4:
        raise ValueError(
            f"bbox must have at least 4 comma-separated values: {bbox_str!r}"
        )
    return float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
