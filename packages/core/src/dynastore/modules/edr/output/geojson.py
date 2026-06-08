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

"""EDR GeoJSON output writer for point (position) queries."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Optional


def write_position_geojson(
    lon: float,
    lat: float,
    datetime_val: Optional[str],
    band_names: List[str],
    values: Dict[int, Optional[float]],
) -> Iterator[bytes]:
    """Emit a GeoJSON FeatureCollection with a single point feature."""
    properties: Dict[str, Any] = {}
    if datetime_val:
        properties["datetime"] = datetime_val
    for idx, name in enumerate(band_names, start=1):
        properties[name] = values.get(idx)

    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": properties,
    }
    doc = {"type": "FeatureCollection", "features": [feature]}
    yield json.dumps(doc).encode("utf-8")
