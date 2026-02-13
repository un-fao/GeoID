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

# dynastore/models/shared_models.py

from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, Annotated, Self, Union, get_args, TypeVar

from pydantic import BaseModel, Field, HttpUrl, field_validator, ConfigDict, AliasChoices

from dynastore.tools.db import validate_sql_identifier
from dynastore.models.localization import Language, T, LocalizedDTO, LocalizedText, LocalizedKeywords, LocalizedLicense, LocalizedExtraMetadata, get_language_object, STAC_LANGUAGE_EXTENSION_URI

# --- Shared Constants ---
SYSTEM_CATALOG_ID = "_system_"
SYSTEM_CATALOG_TITLE = "System Catalog"
SYSTEM_SCHEMA = "catalog"  # Physical schema for system-level tables
SYSTEM_LOGS_TABLE = "system_logs"

# --- Reusable Internationalization Types ---

# Generic type variable for the content (e.g., str, List[str], etc.)
ContentT = TypeVar("ContentT")

# Type alias for a field that can be either:
# 1. A localized value (ContentT) for the current context's language.
# 2. An internationalized dictionary (Dict[str, ContentT]) mapping language codes to values.
# usage: title: Optional[Internationalized[str]] = None
Internationalized = Union[Dict[str, ContentT], ContentT]


# --- OGC / STAC Aligned Primitives ---

class Link(BaseModel):
    """Represents a generic link object, compliant with OGC/STAC standards."""
    href: str
    rel: str
    type: Optional[str] = None
    title: Optional[str] = None
    hreflang: Optional[str] = Field(None, description="The language of the resource at the link destination (RFC 5646).")


class Provider(BaseModel):
    name: str
    description: Optional[str] = None
    roles: Optional[List[str]] = None
    url: Optional[HttpUrl] = None


class TemporalExtent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    interval: List[List[Optional[datetime]]]

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: List) -> List:
        """
        Ensures the temporal interval is never an empty list and that start
        times are before end times.
        """
        if not v:
            return [[None, None]]
        
        for item in v:
            start, end = item
            if start and end and start > end:
                raise ValueError(f"Invalid temporal interval: start time '{start}' is after end time '{end}'.")
        return v


class SpatialExtent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    bbox: List[List[float]] = Field(default=[[0.0, 0.0, 0.0, 0.0]])

    @field_validator("bbox")
    @classmethod
    def validate_bbox(cls, v: List) -> List:
        """
        Validates that each bounding box is logically and geographically correct.
        """
        for box in v:
            minx, miny, maxx, maxy = box
            if minx > maxx:
                raise ValueError(f"Invalid bounding box: minx ({minx}) cannot be greater than maxx ({maxx}).")
            if miny > maxy:
                raise ValueError(f"Invalid bounding box: miny ({miny}) cannot be greater than maxy ({maxy}).")
            if not (-180 <= minx <= 180 and -180 <= maxx <= 180):
                raise ValueError(f"Invalid longitude in bounding box: values must be between -180 and 180.")
            if not (-90 <= miny <= 90 and -90 <= maxy <= 90):
                raise ValueError(f"Invalid latitude in bounding box: values must be between -90 and 90.")
        return v


class Extent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    spatial: SpatialExtent
    temporal: TemporalExtent

    @field_validator("spatial", "temporal", mode="before")
    @classmethod
    def ensure_model_instances(cls, v: Any, info) -> Any:
        """
        Ensures spatial and temporal are proper model instances.
        Handles cases where they might be dicts from JSON storage.
        """
        field_name = info.field_name
        if isinstance(v, dict):
            if field_name == "spatial":
                return SpatialExtent(**v)
            elif field_name == "temporal":
                return TemporalExtent(**v)
        return v


class LocalizableModelMixin:
    """A mixin to provide localization methods to Pydantic models."""

    def localize(self, lang: str, include_language_keys: bool = False) -> Tuple[Dict[str, Any], Set[str]]:
        """
        Converts this Pydantic model to a dict with localized fields resolved,
        recursively localizing fields that are localization-aware.

        Args:
            lang: Requested language.
            include_language_keys: If True, returns resolved values wrapped in a language-keyed dict.
        
        Returns:
            A tuple of (localized_dictionary, set_of_available_languages).
        """
        if not self:
            return {}, set()
        
        # Use by_alias=True to respect field aliases like 'id' for 'code'
        data = self.model_dump(by_alias=True, exclude_none=True)
        
        available_languages: Set[str] = set()

        # Iterate through model fields to find and resolve localizable content
        for field_name, field_info in self.__class__.model_fields.items():
            original_value = getattr(self, field_name, None)
            if original_value is None:
                continue

            serialization_alias = field_info.serialization_alias or field_name

            if isinstance(original_value, LocalizedDTO):
                available_languages.update(original_value.get_available_languages())
                resolved = original_value.resolve(lang, include_language_keys=include_language_keys)
                if resolved is not None:
                    data[serialization_alias] = resolved
                elif serialization_alias in data:
                    del data[serialization_alias]

            elif hasattr(original_value, 'localize') and callable(getattr(original_value, 'localize')):
                localized_data, sub_langs = original_value.localize(lang, include_language_keys=include_language_keys)
                data[serialization_alias] = localized_data
                available_languages.update(sub_langs)
            elif isinstance(original_value, list) and original_value:
                if hasattr(original_value[0], 'localize') and callable(getattr(original_value[0], 'localize')):
                    localized_list = []
                    for item in original_value:
                        item_data, item_langs = item.localize(lang, include_language_keys=include_language_keys)
                        localized_list.append(item_data)
                        available_languages.update(item_langs)
                    data[serialization_alias] = localized_list

        return data, available_languages

    def get_available_languages(self, field_name: str) -> List[str]:
        """Returns a list of available language codes for a given localized field."""
        field_value = getattr(self, field_name, None)
        if isinstance(field_value, LocalizedDTO):
            return list(field_value.get_available_languages())
        return []

    def merge_localized_updates(self, updates: Union[Dict[str, Any], BaseModel], lang: str) -> Self:
        """
        Merges updates into a model, delegating to fields with their own merge logic.
        Returns a new, updated model instance.
        """
        if isinstance(updates, BaseModel):
            # Convert model to dict for iteration. 
            # Since we have a full model, we treat this as a full replacement for localized fields.
            updates = updates.model_dump(by_alias=True, exclude_none=True)
            lang = "*"

        # Dump current model to dict first
        updated_data = self.model_dump(by_alias=True)

        for field_name, new_value in updates.items():
            if new_value is None:
                updated_data[field_name] = None
                continue
            
            # Check if we need to delegate merge logic (e.g. for Localized fields)
            # We need to access the field object on the original model instance to check for 'merge_updates'
            current_field_value = getattr(self, field_name, None)

            if hasattr(current_field_value, 'merge_updates'):
                updated_field_obj = current_field_value.merge_updates(new_value, lang)
                # merge_updates returns an OBJECT (e.g. LocalizedText). 
                # We must dump it to compatible dict/value for the data dict.
                if isinstance(updated_field_obj, BaseModel):
                    updated_data[field_name] = updated_field_obj.model_dump(by_alias=True, exclude_none=True)
                else:
                    updated_data[field_name] = updated_field_obj
            elif current_field_value is None:
                field_info = self.model_fields.get(field_name)
                if field_info:
                    field_type = field_info.annotation
                    if hasattr(field_type, '__origin__') and field_type.__origin__ is Union:
                         from typing import get_args
                         for arg in get_args(field_type):
                            if arg is not type(None):
                                field_type = arg
                                break
                    
                    if isinstance(field_type, type) and hasattr(field_type, 'merge_updates'):
                        empty_instance = field_type()
                        updated_field_obj = empty_instance.merge_updates(new_value, lang)
                        if isinstance(updated_field_obj, BaseModel):
                             updated_data[field_name] = updated_field_obj.model_dump(by_alias=True, exclude_none=True)
                        else:
                             updated_data[field_name] = updated_field_obj
                    else:
                        updated_data[field_name] = new_value
                else:
                    updated_data[field_name] = new_value
            else:
                updated_data[field_name] = new_value
        
        return self.__class__.model_validate(updated_data)

    @classmethod
    def create_from_localized_input(cls, data: Dict[str, Any], lang: str) -> Self:
        """
        Factory method to create a model instance from a dictionary that may contain
        single-language values for its localized fields.
        
        Args:
            data: The input dictionary.
            lang: The language to apply to simple string/list values.
            
        Returns:
            A new instance of the model.
        """
        if lang == '*' or not data:
            return cls.model_validate(data)
        
        processed_data = dict(data)
        
        for field_name, field_info in cls.model_fields.items():
            if field_name not in processed_data or processed_data[field_name] is None:
                continue
            
            value = processed_data[field_name]
            
            # Get the actual type, handling Optional[T]
            field_type = field_info.annotation
            if hasattr(field_type, '__origin__') and field_type.__origin__ is Union:
                field_type = next((arg for arg in get_args(field_type) if arg is not type(None)), None)

            if field_type and hasattr(field_type, 'delocalize_input') and callable(getattr(field_type, 'delocalize_input')):
                processed_data[field_name] = field_type.delocalize_input(value, lang)
                
        return cls.model_validate(processed_data)

class LicenseInfo(BaseModel, LocalizableModelMixin):
    license_id: str = Field(..., description="SPDX License Identifier")
    is_osi_compliant: bool = Field(False, description="Whether the license is OSI compliant")
    localized_content: Optional[LocalizedLicense] = Field(
        None,
        description="Localized license details"
    )

    def merge_updates(self, updates: Union[str, Dict[str, Any]], lang: str) -> 'LicenseInfo':
        """Merges updates into this LicenseInfo, returning a new instance."""
        merged_data = self.model_dump(exclude_unset=True)

        if isinstance(updates, str):
            merged_data['license_id'] = updates
            return LicenseInfo.model_validate(merged_data)

        if isinstance(updates, dict):
            for key in ['license_id', 'is_osi_compliant']:
                if key in updates:
                    merged_data[key] = updates[key]

            if 'localized_content' in updates and updates['localized_content'] is not None:
                current_localized_dto = self.localized_content or LocalizedLicense()
                updated_localized_dto = current_localized_dto.merge_updates(updates['localized_content'], lang)
                merged_data['localized_content'] = updated_localized_dto
            
            return LicenseInfo.model_validate(merged_data)

        return self.model_copy(deep=True)

    @classmethod
    def delocalize_input(cls, value: Any, lang: str) -> Dict[str, Any]:
        """Wraps a simple SPDX string ID into a valid LicenseInfo structure."""
        if isinstance(value, str):
            return {"license_id": value}
        # If it's a dict or other type, return as is for Pydantic to validate.
        return value


class LocalizedFieldsBase(BaseModel, LocalizableModelMixin):
    """
    Model for handling localized metadata updates.
    """
    title: Optional[LocalizedText] = None
    description: Optional[LocalizedText] = None
    keywords: Optional[LocalizedKeywords] = None
    license: Optional[LicenseInfo] = None
    extra_metadata: Optional[LocalizedExtraMetadata] = None

    @field_validator("title", "description", mode="before")
    @classmethod
    def wrap_localized_text(cls, v: Any) -> Any:
        """Wraps a string in a default 'en' dict if it's not already a dict."""
        if isinstance(v, str):
            return {Language.EN.value: v}
        return v

    @field_validator("keywords", mode="before")
    @classmethod
    def wrap_keywords(cls, v: Any) -> Any:
        """Wraps a list of strings in a default 'en' dict if it's not already a dict."""
        if isinstance(v, list) and v and all(isinstance(i, str) for i in v):
            return {Language.EN.value: v}
        return v

    @field_validator("license", mode="before")
    @classmethod
    def wrap_license(cls, v: Any) -> Any:
        """Wraps a string license in a LicenseInfo object if it's not already a dict/model."""
        if isinstance(v, str):
            return {"license_id": v}
        return v

    @field_validator("extra_metadata", mode="before")
    @classmethod
    def wrap_extra_metadata(cls, v: Any) -> Any:
        """Wraps a dict in a LocalizedExtraMetadata object if it's a flat dict."""
        if isinstance(v, dict):
            from dynastore.models.localization import _LANGUAGE_METADATA
            # If keys match known languages, assume it's already localized
            if any(k in _LANGUAGE_METADATA for k in v.keys()):
                return v
            # Otherwise treat as default language content
            return {Language.EN.value: v}
        return v


# --- Core Information Models (Catalog & Collection) ---
class BaseMetadata(LocalizedFieldsBase):
    model_config = ConfigDict(extra='allow', from_attributes=True, populate_by_name=True)

    id: str = Field(..., description="A unique logical identifier.")

    @field_validator("id")
    @classmethod
    def validate_id_format(cls, v: str) -> str:
        """
        Ensures the ID is a valid, unquoted, and non-reserved PostgreSQL identifier.
        """
        # Step 1: Basic SQL identifier validation (format, length, SQL keywords)
        validated_code = validate_sql_identifier(v)

        return validated_code


class Catalog(BaseMetadata):
    """
    Represents a top-level container for data.
    Maps to a logical concept, backed by a physical schema.
    """
    type: str = "Catalog"
    conformsTo: Optional[List[str]] = Field(None, description="A list of conformance classes implemented by this catalog.")
    links: Optional[List[Link]] = Field(default_factory=list)


class CatalogUpdate(LocalizedFieldsBase):
    """
    Model for updating a catalog (code/id comes from URL path).
    """
    model_config = ConfigDict(extra='allow', from_attributes=True, populate_by_name=True)
    links: Optional[List[Link]] = Field(default_factory=list)


class Collection(BaseMetadata):
    """
    Represents a logical grouping of related geospatial items (features).
    """
    type: str = "Collection"
    providers: Optional[List[Provider]] = Field(None)
    extent: Optional[Extent] = Field(None)
    summaries: Optional[Dict[str, Any]] = Field(None)

    # Reserved names validation removed as per new architecture


class CollectionUpdate(LocalizedFieldsBase):
    """
    Model for updating a collection (code/id comes from URL path).
    """
    model_config = ConfigDict(extra='allow', from_attributes=True, populate_by_name=True)
    
    providers: Optional[List[Provider]] = Field(None)
    extent: Optional[Extent] = Field(None)
    summaries: Optional[Dict[str, Any]] = Field(None)



# --- Generic GeoJSON Models ---

class GeoJSONGeometry(BaseModel):
    type: str
    coordinates: Any

class Feature(BaseModel):
    type: Literal["Feature"] = "Feature"
    id: str
    geometry: Optional[GeoJSONGeometry] = None
    properties: Dict[str, Any]
    links: Optional[List[Link]] = None

class FeatureCollection(BaseModel):
    """
    A GeoJSON FeatureCollection, compliant with OGC API - Features Part 1.
    """
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[Feature]
    links: Optional[List[Link]] = None
    numberMatched: Optional[int] = Field(None, description="The total number of features matching the query.")
    numberReturned: Optional[int] = Field(None, description="The number of features returned in this response.")
    timeStamp: Optional[str] = Field(None, description="A timestamp of when the response was generated.") # Recommended by Part 1


# --- OGC API Common Models (e.g., for Filtering/CQL) ---

class FunctionDescription(BaseModel):
    """Describes a single supported CQL2 function."""
    name: str = Field(..., description="The name of the function.")
    returns: List[str] = Field(..., description="The return type(s) of the function.")
    arguments: List[Dict[str, List[str]]] = Field(..., description="The arguments the function accepts.")


class FunctionsResponse(BaseModel):
    """A response model for the /functions endpoint."""
    functions: List[FunctionDescription]
    links: Optional[List[Link]] = None


class OutputFormatEnum(str, Enum):
    """Supported output formats for feature retrieval."""
    GEOJSON = "geojson"
    JSON = "json"
    CSV = "csv"
    GEOPACKAGE = "gpkg"
    SHAPEFILE = "shp"
    PARQUET = "parquet"
    GML = "gml"


class EventType(str, Enum):
    """A base class for creating extensible, strongly-typed event enums."""
    pass


class ItemDataForDB(BaseModel):
    """
    A Pydantic model representing the processed item data ready for database insertion.
    It includes core fields expected by shared_queries.insert_or_update_feature
    and allows for arbitrary extra fields (e.g., spatial indices, extension properties).
    """
    model_config = ConfigDict(extra='allow')

    external_id: str
    attributes: Dict[str, Any]
    wkb_hex_processed: str
    geom_type: str
    content_hash: str
    
    # These are intermediate values used to construct PostGIS geometries
    bbox_coords: Optional[List[float]] = None

    # These are used to construct the PostGIS TSTZRANGE 'validity' column
    valid_from: Optional[datetime] = None
    valid_to: Optional[datetime] = None

    # Ingestion-related metadata
    asset_id: Optional[str] = None