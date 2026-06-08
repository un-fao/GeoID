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

"""Protocol for fetching feature geometries from a collection store.

Used by the tile-content pipeline (b3dm / glb) to retrieve polygon WKB
and height attributes for a specific set of feature IDs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence, runtime_checkable


@dataclass(frozen=True)
class FeatureGeometry:
    """Raw geometry + height attribute for one feature."""

    feature_id: str
    geom_wkb: bytes
    height: float = 0.0


@runtime_checkable
class GeometryFetcherProtocol(Protocol):
    """Fetch WKB geometries + height attrs for an explicit feature-ID list."""

    async def get_geometries(
        self,
        catalog_id: str,
        collection_id: str,
        feature_ids: Sequence[str],
    ) -> Sequence[FeatureGeometry]:
        """Return geometries for *feature_ids*.

        Missing or null-geometry features are silently omitted.
        """
        ...
