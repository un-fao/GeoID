#    Copyright 2025 FAO
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

"""
Read hints for storage driver routing.

Standard hints are defined in ``ReadHint``. Drivers may register
custom hints at startup via ``register_hint()``.
"""

from enum import StrEnum
from typing import Dict


class ReadHint(StrEnum):
    """Standard read hints for storage driver routing."""

    DEFAULT = "default"
    METADATA = "metadata"      # collection-level metadata (title, description, extent)
    SEARCH = "search"
    FEATURES = "features"
    GRAPH = "graph"
    ANALYTICS = "analytics"
    CACHE = "cache"
    ENRICHMENT = "enrichment"  # cross-driver filtering/enrichment (collection and item level)


_CUSTOM_HINTS: Dict[str, str] = {}


def register_hint(name: str, description: str) -> None:
    """Register a custom hint. Drivers call this in their ``lifespan()``."""
    _CUSTOM_HINTS[name] = description


def get_registered_hints() -> Dict[str, str]:
    """Return all hints (standard + custom) with descriptions."""
    result = {h.value: f"Standard: {h.name}" for h in ReadHint}
    result.update(_CUSTOM_HINTS)
    return result
