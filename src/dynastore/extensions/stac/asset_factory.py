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

# dynastore/extensions/stac/asset_factory.py

import pystac
from pydantic import BaseModel
from pydantic import BaseModel, ConfigDict
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol,
                    Type, TypeVar, Union, cast, runtime_checkable)
from dynastore.extensions import get_extension_instance
from dynastore.modules.stac.stac_config import StacPluginConfig
from fastapi import Request

import logging
logger = logging.getLogger(__name__)


class AssetContext(BaseModel):
    """A structured model for the context required to generate dynamic assets."""
    # Allow arbitrary types like fastapi.Request, which Pydantic cannot generate a schema for.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    base_url: str
    catalog_id: str
    collection_id: str
    request: Request
    stac_config: StacPluginConfig
    asset_id: Optional[str] = None

# Safely import service classes for type checking and name resolution.
try:
    from dynastore.extensions.features.features_service import OGCFeaturesService
except ImportError:
    OGCFeaturesService = None
try:
    from dynastore.extensions.maps.maps_service import MapsService
except ImportError:
    MapsService = None
try:
    from dynastore.extensions.tiles.tiles_service import TilesService
except ImportError:
    TilesService = None

# --- Asset Provider Functions ---
# We use a Union to allow these to apply to both Items and Collections (STAC 1.0.0+)
StacTarget = Union[pystac.Item, pystac.Collection]

def _add_ogc_features_asset(item: StacTarget, context: AssetContext):
    """Adds an asset for the OGC API - Features endpoint."""
    if not OGCFeaturesService: return
    instance = get_extension_instance(OGCFeaturesService.get_name())
    if not instance: return

    base_url = context.base_url
    asset_href = f"{base_url}{instance.router.prefix}/catalogs/{context.catalog_id}/collections/{context.collection_id}/items/{item.id}"
    item.add_asset(
        key="geojson",
        asset=pystac.Asset(href=asset_href, title="OGC API Feature", media_type=pystac.MediaType.GEOJSON, roles=["data"])
    )

def _add_ogc_maps_asset(item: StacTarget, context: AssetContext):
    """Adds a dynamic asset for the OGC API - Maps render of this item."""
    if not MapsService: return
    instance = get_extension_instance(MapsService.get_name())
    if not instance: return

    base_url = context.base_url
    # Check if a style was requested in the original STAC item request
    style_param = context.request.query_params.get('style')
    style_query_str = f"&style={style_param}" if style_param else ""

    if item.bbox is None:
        return

    bbox_str = ",".join(map(str, item.bbox))
    asset_href = (f"{base_url}{instance.router.prefix}/{context.catalog_id}/map"
                  f"?collections={context.collection_id}&bbox={bbox_str}"
                  f"&crs=EPSG:4326&width=512&height=512{style_query_str}")

    item.add_asset(
        key="map_preview",
        asset=pystac.Asset(href=asset_href, title="Rendered Map Preview", media_type="image/png", roles=["thumbnail", "visual"])
    )

def _add_ogc_tiles_asset(item: StacTarget, context: AssetContext):
    """Adds a dynamic asset for the OGC API - Tiles endpoint."""
    if not TilesService: return
    instance = get_extension_instance(TilesService.get_name())
    if not instance: return
    
    base_url = context.base_url
    # This is a template URL, as tiles are accessed by Z/X/Y
    asset_href = f"{base_url}{instance.router.prefix}/{context.catalog_id}/tiles/{{z}}/{{x}}/{{y}}.mvt?collections={context.collection_id}"
    item.add_asset(
        key="vector_tiles",
        asset=pystac.Asset(href=asset_href, title="Vector Tiles (MVT)", media_type="application/vnd.mapbox-vector-tile", roles=["tiles"])
    )

def _add_source_file_asset(item: StacTarget, context: AssetContext):
    """
    Adds a dynamic asset pointing to the original source file.
    The actual HREF resolution (proxy vs direct) is handled at runtime.
    """
    asset_id = context.asset_id or item.properties.get("asset_id")
    if not asset_id:
        return

    # We generate a stable internal link that the STAC service will resolve
    # or redirect based on runtime policy/configuration.
    base_url = context.base_url
    asset_href = f"{base_url}/stac/catalogs/{context.catalog_id}/collections/{context.collection_id}/assets/{asset_id}/source"

    item.add_asset(
        "source_file",
        pystac.Asset(href=asset_href, title="Original Source File", roles=["data", "source"])
    )


def _add_source_asset_to_collection(collection: pystac.Collection, context: AssetContext):
    """
    Adds the source asset file to a virtual collection.
    Used when a collection represents an ingested asset.
    """
    if not context.asset_id:
        return
    
    base_url = context.base_url
    asset_href = f"{base_url}/stac/catalogs/{context.catalog_id}/collections/{context.collection_id}/assets/{context.asset_id}/source"
    
    collection.add_asset(
        "source_file",
        pystac.Asset(
            href=asset_href,
            title="Original Ingested File",
            roles=["source", "data"]
        )
    )


# --- Asset Provider Registry ---
ASSET_PROVIDERS: List[Callable[[StacTarget, AssetContext], None]] = [
    _add_ogc_features_asset,
    _add_ogc_maps_asset,
    _add_ogc_tiles_asset,
    _add_source_file_asset,
]

COLLECTION_ASSET_PROVIDERS: List[Callable[[pystac.Collection, AssetContext], None]] = [
    _add_source_asset_to_collection,
]

def add_dynamic_assets(item: StacTarget, context: AssetContext):
    """
    Iterates through registered asset providers and adds assets to a STAC Item or Collection
    if the corresponding service extensions are enabled and loaded.
    """
    # Determine if this is a Collection or Item
    is_collection = isinstance(item, pystac.Collection)
    
    providers = COLLECTION_ASSET_PROVIDERS if is_collection else ASSET_PROVIDERS
    
    for provider in providers:
        try:
            provider(item, context)
        except Exception as e:
            # Log error but don't fail the entire item generation
            logger.error(f"Error adding dynamic asset with {provider.__name__}: {e}")
