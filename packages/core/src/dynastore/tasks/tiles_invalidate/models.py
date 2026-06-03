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

from pydantic import BaseModel, Field
from typing import Optional, List, Tuple


class TileInvalidateRequest(BaseModel):
    """Input payload for the Tile Cache Invalidation Process.

    Carries only the fields the delete-only path needs — no render/format
    options (those belong to the preseed/seed path). The task deletes every
    cached tile that overlaps the supplied bboxes across the served TMS ids
    and zoom range; no MVT render, no save.
    """

    catalog_id: str = Field(..., description="The catalog identifier to process.")
    collection_id: Optional[str] = Field(
        None,
        description=(
            "Optional collection identifier. If omitted, applies to all "
            "collections in the catalog that are TMS-configured."
        ),
    )
    update_bbox: Optional[List[Tuple[float, float, float, float]]] = Field(
        None,
        description=(
            "Bounding boxes (WGS84 EPSG:4326) whose cached tiles should be "
            "deleted. When omitted the task falls back to the preseed/runtime "
            "config bbox, then the world bbox. The DELETE/geometry-MOVE prior "
            "extents (a feature's pre-write footprint) are unioned into this "
            "list at enqueue time, so the task treats them uniformly."
        ),
    )
    tms_ids: Optional[List[str]] = Field(
        None,
        description=(
            "TMS IDs whose cached tiles to invalidate. Overrides the collection's "
            "TilesConfig.supported_tms_ids when provided."
        ),
    )
