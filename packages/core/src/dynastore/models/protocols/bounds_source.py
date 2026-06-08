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

"""Protocol for 'give me 3D bounding boxes for a collection' producers.

Used by VolumesService.get_tileset_json to source the FeatureBounds list
the tile-hierarchy builder consumes. Real implementations:

- Sidecar-backed: queries the geometries sidecar for ST_3DExtent of each
  feature in a collection, optionally filtered by bbox/limit.
- Task-backed: pre-computed tileset emitted by VolumesTilesetBuildTask
  (see Pass 5 spec).
- Fixture: tests inject a list directly.

Phase 5d (this PR) ships the Protocol + an EmptyBoundsSource default so
the HTTP surface keeps working when no real producer is registered.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Protocol, Sequence, runtime_checkable

if TYPE_CHECKING:
    from dynastore.modules.volumes.bounds import FeatureBounds


@runtime_checkable
class BoundsSourceProtocol(Protocol):
    """Producer of 3D bounding boxes for a collection's features."""

    async def get_bounds(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: Optional[int] = None,
    ) -> Sequence[FeatureBounds]:
        """Return per-feature 3D bboxes. Empty sequence when no data."""
        ...


class EmptyBoundsSource:
    """Default no-op source — returns no features.

    Picked up by VolumesService when no other BoundsSourceProtocol is
    registered. Keeps the /tileset.json endpoint functional (empty
    skeleton) without forcing every test or dev environment to wire a
    real producer.
    """

    async def get_bounds(
        self,
        catalog_id: str,
        collection_id: str,
        *,
        limit: Optional[int] = None,
    ) -> Sequence[FeatureBounds]:
        return []
