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

"""Configuration for the generic Item Metadata sidecar."""

from typing import Literal

from dynastore.modules.storage.drivers.pg_sidecars.base import SidecarConfig, SidecarConfigRegistry


class ItemMetadataSidecarConfig(SidecarConfig):
    """Configuration for per-item multilanguage metadata sidecar.

    This sidecar stores generic item-level metadata (title, description,
    keywords) with internationalization support, plus extension/asset columns
    that may be consumed by downstream sidecars (e.g. STAC overlay).
    """

    # Literal-typed discriminator — required by Pydantic for the
    # Annotated[Union[...], Discriminator("sidecar_type")] dispatch on
    # CollectionPostgresqlDriverConfig.sidecars.
    sidecar_type: Literal["item_metadata"] = "item_metadata"


SidecarConfigRegistry.register("item_metadata", ItemMetadataSidecarConfig)
