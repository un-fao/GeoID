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

# dynastore/extensions/features/ogc_generator.py

import logging
logger = logging.getLogger(__name__)

import uuid
from typing import Dict, Any, Optional, List
from dynastore.tools.geospatial import process_geometry, GeometryProcessingError
from fastapi import Request, HTTPException, status
from datetime import datetime, timezone
from shapely import wkb
from shapely.geometry import shape, mapping

from . import ogc_models
from dynastore.tools.geospatial import calculate_spatial_indices
from dynastore.modules.catalog.catalog_config import CollectionPluginConfig, GeometryStorageConfig
from dynastore.modules.catalog.models import Collection as CoreCollection # Import CoreCollection for type hinting
import dynastore.modules.catalog.catalog_module as catalog_manager
from dynastore.models.localization import LocalizedText, Language
from dynastore.modules.catalog.tools import prepare_item_for_db
from dynastore.extensions.tools.url import get_root_url
from dynastore.models.shared_models import Link
from dynastore.extensions.tools.conformance import get_active_conformance, Conformance
from dynastore.models.protocols import CatalogsProtocol, ItemsProtocol
from dynastore.tools.discovery import get_protocol
from dynastore.extensions.tools.url import (get_parent_from_url, get_url)

def create_landing_page(request: Request, language: str = Language.EN.value) -> ogc_models.LandingPage:
    """Generates the response for the API's landing page."""
    base_url = get_root_url(request)
    links = [
        Link(href=f"{base_url}/features/", rel="self", type="application/json", title="This document"),
        Link(href=f"{base_url}/features/catalogs", rel="catalogs", type="application/json", title="List of catalogs"),
        Link(href=f"{base_url}/conformance", rel="conformance", type="application/json", title="Conformance classes"),
        Link(href=f"{base_url}/api", rel="service-doc", type="application/json", title="API documentation"),
    ]
    return ogc_models.LandingPage(
        title="DynaStore OGC API Features",
        description="Access to geospatial data via OGC API - Features",
        links=links
    )

def _db_row_to_ogc_feature(row: Dict, catalog_id: str, collection_id: str, root_url: str) -> ogc_models.Feature:
    """Converts a database row into an OGC Feature object."""
    # Handle both SQLAlchemy Row objects (which have _mapping) and standard dicts.
    if hasattr(row, '_mapping'):
        row_dict = dict(row._mapping)
    else:
        row_dict = dict(row)

    geom_wkb = row_dict.get('geom')
    geometry_model = None
    if geom_wkb:
        geom_shapely = wkb.loads(geom_wkb)
        geometry_model = ogc_models.GeoJSONGeometry(**mapping(geom_shapely))

    feature_id = str(row_dict.get('external_id') or row_dict.get('geoid'))

    # Construct the full, correct URLs for the feature and its parent collection.
    collection_url = f"{root_url}/features/catalogs/{catalog_id}/collections/{collection_id}"
    links = [
        Link(href=f"{collection_url}/items/{feature_id}", rel="self", type="application/geo+json"),
        Link(href=collection_url, rel="collection", type="application/json")
    ]
    return ogc_models.Feature(id=feature_id, geometry=geometry_model, properties=row_dict.get('attributes', {}), links=links)

async def create_queryables_response(request: Request, catalog_id: str, collection_id: str, columns: list, language: str = "en") -> ogc_models.Queryables:
    """Generates the queryables response for a collection."""
    catalogs = get_protocol(CatalogsProtocol)
    collection: Optional[CoreCollection] = await catalogs.get_collection(catalog_id, collection_id)
    if not collection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Collection not found")

    queryables_url = get_url(request)

    properties = {
        "geometry": {
            "type": "object",
            "$ref": "https://geojson.org/schema/Geometry.json",
            "description": "The geometry of the feature."
        }
    }

    # Add top-level columns
    for col_name in columns:
        properties[col_name] = {
            "title": col_name,
            "type": columns[col_name].type,
            # "description": columns[col_name].description
        }
    
    # A real implementation would inspect the 'attributes' JSONB schema if available
    localized, _ = collection.localize(language)
    return ogc_models.Queryables(
        id=queryables_url,
        title=localized.get('title') or collection_id,
        properties=properties
    )

def _process_feature_for_db(feature: ogc_models.FeatureDefinition, layer_config: CollectionPluginConfig) -> Dict[str, Any]:
    """
    Validates and prepares a feature for database insertion, raising
    HTTPExceptions on failure.
    """
    if not feature.geometry: 
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Feature must have a valid geometry.")
    
    try:
        # Convert Pydantic GeoJSON model to dict
        geom_dict = feature.geometry.model_dump()
        
        # Defensively look for ID in top level or properties
        external_id = feature.id or feature.properties.get('id')
        logger.debug(f"Processing feature for DB: id={external_id}")

        return prepare_item_for_db(
            feature_geometry=geom_dict,
            feature_properties=feature.properties,
            layer_config=layer_config,
            external_id=external_id
        )
    except ValueError as e:
        logger.error(f"Failed to process feature: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except KeyError as e:
        logger.error(f"Failed to process feature due to KeyError: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Internal config error: Missing key {e}")