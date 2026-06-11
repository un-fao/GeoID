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

"""OGC API - 3D GeoVolumes draft 22-029 wire shapes.

All 22-029 response models live in this single file so that future field
renames mandated by the evolving draft require changes in exactly one place.

The shared ``Link`` model from ``dynastore.models.shared_models`` is reused
for the ``links`` array on ``ThreeDContainer`` — it is the repo-wide OGC link
primitive used by every other extension.
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from dynastore.models.shared_models import Link


# ---------------------------------------------------------------------------
# ContentExtent
# ---------------------------------------------------------------------------

#: Default CRS for 3D bounding-box coordinates (WGS 84 + ellipsoidal height).
_DEFAULT_CRS = "http://www.opengis.net/def/crs/OGC/0/CRS84h"


class ContentExtent(BaseModel):
    """3D bounding box and optional CRS for a GeoVolumes container.

    ``bbox`` must contain exactly 6 values: [minX, minY, minZ, maxX, maxY, maxZ]
    in the coordinate order of ``crs`` (default: CRS84h — lon, lat, height).
    """

    model_config = ConfigDict(populate_by_name=True)

    bbox: List[float] = Field(
        description="Minimum bounding box [minX, minY, minZ, maxX, maxY, maxZ].",
    )
    crs: str = Field(
        default=_DEFAULT_CRS,
        description="CRS URI for the bbox coordinates.",
    )

    @field_validator("bbox")
    @classmethod
    def _validate_bbox_arity(cls, v: List[float]) -> List[float]:
        if len(v) != 6:
            raise ValueError(
                f"bbox must have exactly 6 elements [minX, minY, minZ, maxX, maxY, maxZ], "
                f"got {len(v)}."
            )
        return v


# ---------------------------------------------------------------------------
# ContentLink
# ---------------------------------------------------------------------------


class ContentLink(BaseModel):
    """Link to a 3D content resource (e.g. 3D Tiles, I3S, CityJSON).

    Field names match the 22-029 wire schema verbatim.
    """

    model_config = ConfigDict(populate_by_name=True)

    rel: str = Field(description="Link relation type URI or registered token.")
    href: str = Field(description="Target URL of the content resource.")
    type: Optional[str] = Field(default=None, description="Media type of the content resource.")
    title: Optional[str] = Field(default=None, description="Human-readable label for the link.")


# ---------------------------------------------------------------------------
# ChildRef
# ---------------------------------------------------------------------------


class ChildRef(BaseModel):
    """Reference to a child 3D container (used in ``ThreeDContainer.children``).

    Carries the minimal identity fields needed for a client to navigate into
    a nested container without fetching the full child document.
    """

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description="Identifier of the child container.")
    title: Optional[str] = Field(default=None, description="Human-readable label for the child.")
    collectionType: str = Field(  # noqa: N815 — wire name
        description="Collection type discriminator (typically '3dcontainer').",
    )


# ---------------------------------------------------------------------------
# ThreeDContainer
# ---------------------------------------------------------------------------


class ThreeDContainer(BaseModel):
    """Root wire shape for an OGC API - 3D GeoVolumes container (22-029 §7.2).

    A container aggregates 3D content links (e.g. 3D Tiles, I3S, CityJSON)
    and optional child containers, enabling hierarchical scene decomposition.
    """

    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(description="Unique identifier for this 3D container.")
    title: Optional[str] = Field(default=None, description="Human-readable title.")
    collectionType: str = Field(  # noqa: N815 — wire name
        default="3dcontainer",
        description="Discriminator field; always '3dcontainer' for this resource type.",
    )
    contentExtent: Optional[ContentExtent] = Field(  # noqa: N815 — wire name
        default=None,
        description="3D bounding box covering all content in this container.",
    )
    content: List[ContentLink] = Field(
        default_factory=list,
        description="Links to the 3D content resources held by this container.",
    )
    children: List[ChildRef] = Field(
        default_factory=list,
        description="References to nested child containers.",
    )
    links: List[Link] = Field(
        default_factory=list,
        description="OGC API navigational links (self, alternate, etc.).",
    )
