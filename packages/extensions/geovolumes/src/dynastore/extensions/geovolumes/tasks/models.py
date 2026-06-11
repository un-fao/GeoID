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

"""Input payload model for the GeoVolumes 3D Tiles generation process."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class GeoVolumesTilesetRequest(BaseModel):
    """Input payload for the geovolumes_tileset OGC Process."""

    catalog_id: str = Field(..., description="The catalog identifier.")
    collection_id: str = Field(..., description="The collection identifier.")
    lod: Optional[str] = Field(
        None,
        description=(
            "Level-of-detail filter.  When supplied, only CityJSON geometries "
            "whose 'lod' attribute matches this value are included."
        ),
    )
