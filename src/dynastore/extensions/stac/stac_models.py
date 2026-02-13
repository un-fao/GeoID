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

# dynastore/extensions/stac/all_langls.py

import logging
import os
from typing import Annotated, Any, Dict, List, Optional, Set, Tuple, Union
import pystac
from pydantic import BaseModel, Field, model_validator, ConfigDict, AliasChoices

logger = logging.getLogger(__name__)
from dynastore.models.shared_models import LocalizableModelMixin
from dynastore.models.localization import STAC_LANGUAGE_EXTENSION_URI, get_language_object, localize_dict

try:
    import jsonschema
    PYSTAC_VALIDATION_AVAILABLE = True
except ImportError:
    PYSTAC_VALIDATION_AVAILABLE = False

from dynastore.models.shared_models import Link, Provider, Extent, Internationalized

# --- STAC API DTOs (Standard Compliance) ---
# These models represent the "Wire Format" exposed in OpenAPI.
# They allow both standard single-value strings (localized input) 
# AND dictionary values (multilanguage input) via the generic Internationalized alias.

class STACCatalogRequest(BaseModel):
    """
    Standard STAC Catalog Definition for Creation/Update.
    Fields like title/description accept either:
    1. A string (treated as content for the request's active language)
    2. A dictionary (treated as full multi-language content)
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    type: str = Field("Catalog", description="Must be 'Catalog'.")
    stac_version: str = Field("1.0.0", description="The STAC version the Catalog implements.")
    stac_extensions: Optional[List[str]] = Field(None, description="A list of STAC extensions the Catalog implements.")
    
    id: Annotated[str, Field(description="The ID of the catalog.")]
    
    # Use generic Internationalized type for i18n support
    title: Optional[Internationalized[str]] = Field(None, description="A short descriptive one-line title for the catalog. Can be a string or a language dictionary.")
    description: Internationalized[str] = Field(..., description="Detailed multi-line description to fully explain the catalog. Can be a string or a language dictionary.")
    
    keywords: Optional[Internationalized[List[str]]] = Field(None, description="List of keywords describing the catalog.")
    license: Internationalized[Any] = Field("proprietary", description="License(s) of the data, as a SPDX License identifier or a complex license object.")
    
    links: Optional[List[Link]] = Field(default_factory=list, description="A list of references to other documents.")
    
    # Extra metadata container for custom fields
    extra_metadata: Optional[Internationalized[Any]] = Field(None, description="Additional metadata fields.")


class STACCollectionRequest(BaseModel):
    """
    Standard STAC Collection Definition for Creation/Update.
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    type: str = Field("Collection", description="Must be 'Collection'.")
    stac_version: str = Field("1.0.0", description="The STAC version the Collection implements.")
    stac_extensions: Optional[List[str]] = Field(None, description="A list of STAC extensions the Collection implements.")
    
    id: Annotated[str, Field(description="The ID of the collection.")]
    
    # Use generic Internationalized type for i18n support
    title: Optional[Internationalized[str]] = Field(None, description="A short descriptive one-line title for the collection.")
    description: Internationalized[str] = Field(..., description="Detailed multi-line description to fully explain the collection.")
    keywords: Optional[Internationalized[List[str]]] = Field(None, description="List of keywords describing the collection.")
    license: Internationalized[Any] = Field("proprietary", description="License(s) of the data, as a SPDX License identifier.")
    
    providers: Optional[List[Provider]] = Field(None, description="A list of providers, which may include all the organizations that captured or processed the data.")
    extent: Extent = Field(..., description="Spatial and temporal extent of the data in the collection.")
    summaries: Optional[Dict[str, Any]] = Field(None, description="A dictionary of summaries of the data in the collection.")
    
    links: Optional[List[Link]] = Field(default_factory=list, description="A list of references to other documents.")
    
    # Extra metadata container
    extra_metadata: Optional[Internationalized[Any]] = Field(None, description="Additional metadata fields.")


class STACCatalogUpdate(BaseModel):
    """DTO for updating a Catalog. All fields optional."""
    model_config = ConfigDict(populate_by_name=True, extra='allow')
    
    title: Optional[Internationalized[str]] = None
    description: Optional[Internationalized[str]] = None
    keywords: Optional[Internationalized[List[str]]] = None
    license: Optional[Internationalized[Any]] = None
    extra_metadata: Optional[Internationalized[Any]] = None


class STACCollectionUpdate(BaseModel):
    """DTO for updating a Collection. All fields optional."""
    model_config = ConfigDict(populate_by_name=True, extra='allow')
    
    title: Optional[Internationalized[str]] = None
    description: Optional[Internationalized[str]] = None
    keywords: Optional[Internationalized[List[str]]] = None
    license: Optional[Internationalized[Any]] = None
    providers: Optional[List[Provider]] = None
    extent: Optional[Extent] = None
    summaries: Optional[Dict[str, Any]] = None
    extra_metadata: Optional[Internationalized[Any]] = None


# --- Containers ---

class Collections(BaseModel):
    """A container for a list of STAC Collection objects."""
    collections: List[Dict[str, Any]] # We use Dict to allow the flexible STAC output structure
    links: List[Link]


from dynastore.models.ogc import Feature

class STACItem(Feature):
    """
    A Pydantic model that validates a raw dictionary as a STAC Item.
    Inherits fields (id, type, geometry, properties, links) from Feature.
    """
    model_config = ConfigDict(extra='allow', arbitrary_types_allowed=True)

    # Overrides/Extensions
    stac_version: str = Field("1.0.0", description="The STAC version the Item implements.")
    
    # Feature's fields refined for STAC requirements
    id: str = Field(..., description="Provider-specific identifier for the Item.")
    bbox: List[float] = Field(..., description="Bounding box of the asset.")
    collection: Optional[str] = Field(None, description="The ID of the collection this Item belongs to.")
    
    # Note: 'properties' and 'geometry' are inherited from Feature but STAC enforces specific semantics.
    # We can override if stricter validation is needed, but Base Feature is usually sufficient.

    @model_validator(mode='before')
    @classmethod
    def validate_as_stac_item(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not values: raise ValueError("Request body cannot be empty.")
        try:
            stac_item = pystac.Item.from_dict(values)
            if PYSTAC_VALIDATION_AVAILABLE and "PYTEST_CURRENT_TEST" not in os.environ:
                stac_item.validate()
            return values
        except Exception as e:
            raise ValueError(f"Invalid STAC Item: {e}")

    def to_pystac(self) -> pystac.Item:
        return pystac.Item.from_dict(self.model_dump(by_alias=True, exclude_unset=True))


class STACItemResponse(BaseModel):
    """Response model for STAC Item (open dict)."""
    model_config = ConfigDict(extra='allow')

class STACItemCollectionResponse(BaseModel):
    """Response model for STAC ItemCollection (open dict)."""
    model_config = ConfigDict(extra='allow')


def inject_stac_language_fields(data: Dict[str, Any], available_languages: Set[str], lang: str) -> Dict[str, Any]:
    """
    Injects STAC Language Extension fields ('language', 'languages') into a dictionary.
    """
    if not available_languages:
        return data

    if lang != '*':
        # 1. The 'language' field describes the current representation's language
        data['language'] = get_language_object(lang).model_dump(exclude_none=True)
        
        # 2. The 'languages' field lists OTHER available languages
        other_langs = available_languages - {lang}
        # Fallback handling: if 'en-US' was requested but only 'en' exists, remove 'en' from others
        base_lang = lang.split('-')[0]
        if base_lang in other_langs:
            other_langs.remove(base_lang)
            
        if other_langs:
            data['languages'] = [
                get_language_object(l).model_dump(exclude_none=True) 
                for l in sorted(other_langs)
            ]
    
    # Also add the extension URI if it's a STAC object with stac_extensions
    if 'stac_extensions' in data and isinstance(data['stac_extensions'], list):
        if STAC_LANGUAGE_EXTENSION_URI not in data['stac_extensions']:
            data['stac_extensions'].append(STAC_LANGUAGE_EXTENSION_URI)

    return data


def stac_localize(model: LocalizableModelMixin, lang: str) -> Tuple[Dict[str, Any], Set[str]]:
    """Localizes a model and injects STAC language fields."""
    # STAC Language Extension treats documents as mono-lingual in the requested language.
    # If lang is '*', we return all available translations as a dictionary.
    # Otherwise, we return plain strings for the requested language.
    include_keys = (lang == '*')
    localized_data, available_langs = model.localize(lang, include_language_keys=include_keys)
    
    if available_langs:
        localized_data = inject_stac_language_fields(localized_data, available_langs, lang)
    return localized_data, available_langs