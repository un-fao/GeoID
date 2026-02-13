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

# dynastore/extensions/features/ogc_models.py

from typing import List, Optional, Dict, Any, Literal, Union
from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from dynastore.models.shared_models import Link, Extent, Provider, Internationalized

# --- OGC API Common / Features DTOs ---

class Conformance(BaseModel):
    conformsTo: List[str]

class CatalogDefinition(BaseModel):
    """
    Request body for creating/updating a Catalog via OGC API.
    Supports both localized strings and multi-language dictionaries via Internationalized generic.
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow', from_attributes=True)
    
    id: Optional[str] = Field(None, description="Unique identifier for the catalog.")
    title: Optional[Internationalized[str]] = Field(None, description="Title of the catalog.")
    description: Optional[Internationalized[str]] = Field(None, description="Description of the catalog.")
    keywords: Optional[Internationalized[List[str]]] = Field(None, description="Keywords.")
    license: Optional[Internationalized[Any]] = Field(None, description="License information.")
    links: Optional[List[Link]] = Field(default_factory=list, description="Links related to the catalog.")
    
    # Extra metadata
    extra_metadata: Optional[Internationalized[Any]] = None


class CollectionDefinition(BaseModel):
    """
    Request body for creating/updating a Collection via OGC API.
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow', from_attributes=True)
    
    id: Optional[str] = Field(None, description="Unique identifier for the collection.")
    title: Optional[Internationalized[str]] = Field(None)
    description: Optional[Internationalized[str]] = Field(None)
    keywords: Optional[Internationalized[List[str]]] = Field(None)
    license: Optional[Internationalized[Any]] = Field(None)
    
    extent: Optional[Extent] = None
    providers: Optional[List[Provider]] = None
    summaries: Optional[Dict[str, Any]] = None
    
    extra_metadata: Optional[Internationalized[Any]] = None


class LandingPage(BaseModel):
    title: str = "DynaStore OGC API"
    description: str = "Access to geospatial data via OGC API Features"
    links: List[Link]

class OGCCollection(BaseModel):
    """Response model for a Collection."""
    id: str = Field(..., description="Collection identifier")
    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link] = []
    extent: Optional[Extent] = None
    itemType: str = "feature"
    crs: List[str] = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]
    
    model_config = ConfigDict(populate_by_name=True, extra='allow')

class Collections(BaseModel):
    links: List[Link]
    collections: List[OGCCollection]
    crs: Optional[List[str]] = None

class Catalogs(BaseModel):
    """Response container for list of catalogs (not strictly OGC Features core but common extension)."""
    links: List[Link]
    catalogs: List[CatalogDefinition] # Reusing definition for list output might need checking fields

class Queryables(BaseModel):
    type: str = "object"
    title: str
    properties: Dict[str, Any]
    schema_: str = Field("http://json-schema.org/draft/2019-09/schema", alias="$schema")
    link: Optional[str] = Field(None, alias="$id")

from dynastore.models.ogc import Feature, FeatureCollection, GeoJSONGeometry

class FeatureDefinition(Feature):
    """Input model for creating a feature."""
    pass

class FeatureCollectionDefinition(FeatureCollection):
    """Input model for bulk creation."""
    features: List[FeatureDefinition]

from pydantic import RootModel

class FeatureOrFeatureCollection(RootModel):
    """Union wrapper for input handling."""
    root: Union[FeatureDefinition, FeatureCollectionDefinition] 

class BulkCreationResponse(BaseModel):
    ids: List[str]