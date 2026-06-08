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

"""Build EDR parameter metadata from STAC item raster:bands."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _select_bands(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    assets = item.get("assets") or {}
    for key in ("data", "coverage"):
        if key in assets and assets[key].get("raster:bands"):
            return assets[key]["raster:bands"]
    for a in assets.values():
        if a.get("raster:bands"):
            return a["raster:bands"]
    return []


def build_parameters(item: Dict[str, Any]) -> Dict[str, Any]:
    """Build EDR parameters dict from STAC item raster:bands."""
    bands = _select_bands(item)
    params: Dict[str, Any] = {}
    for band in bands:
        name = band.get("name", "band")
        unit_label = band.get("unit", "1")
        description = band.get("description", name)
        params[name] = {
            "type": "Parameter",
            "description": {"en": description},
            "unit": {"label": {"en": unit_label}},
            "observedProperty": {
                "id": band.get("name", ""),
                "label": {"en": description},
            },
        }
    return params


def filter_parameters(
    bands: List[Dict[str, Any]],
    parameter_names: Optional[List[str]],
) -> List[Dict[str, Any]]:
    """Filter bands list to only those matching requested parameter names."""
    if not parameter_names:
        return bands
    return [b for b in bands if b.get("name") in parameter_names]
