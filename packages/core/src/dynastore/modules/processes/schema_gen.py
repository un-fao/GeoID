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

"""
Utilities for generating OGC Process definitions from Pydantic models.

This eliminates duplication between Pydantic request models and OGC Process input schemas.
"""

import logging
from typing import Type, Dict, Any, get_origin, get_args
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined
from enum import Enum

logger = logging.getLogger(__name__)


def pydantic_to_process_inputs(model: Type[BaseModel]) -> Dict[str, Any]:
    """
    Convert a Pydantic model to OGC Process inputs schema.
    
    Args:
        model: Pydantic BaseModel class
        
    Returns:
        Dictionary mapping field names to OGC Process input schemas
        
    Example:
        >>> class MyRequest(BaseModel):
        ...     name: str = Field(..., description="User name")
        ...     age: int = Field(default=18, description="User age")
        ...
        >>> inputs = pydantic_to_process_inputs(MyRequest)
        >>> inputs["name"]
        {'title': 'Name', 'schema': {'type': 'string'}, 'description': 'User name'}
    """
    inputs = {}
    
    for field_name, field_info in model.model_fields.items():
        inputs[field_name] = _field_to_process_input(field_name, field_info)
    
    return inputs


def _field_to_process_input(field_name: str, field_info: FieldInfo) -> Dict[str, Any]:
    """
    Convert a single Pydantic field to an OGC Process input definition.
    
    Args:
        field_name: Name of the field
        field_info: Pydantic FieldInfo object
        
    Returns:
        OGC Process input definition dict
    """
    # Get field annotation (type)
    field_type = field_info.annotation
    
    # Build base input definition
    input_def = {
        "title": field_info.title or field_name.replace("_", " ").title(),
        "schema": _type_to_json_schema(field_type, field_info)  # type: ignore[arg-type]
    }
    
    # Add description if available
    if field_info.description:
        input_def["description"] = field_info.description
    
    return input_def


def _type_to_json_schema(field_type: Type, field_info: FieldInfo) -> Dict[str, Any]:
    """
    Convert a Python type annotation to JSON Schema.
    
    Args:
        field_type: Python type annotation
        field_info: Pydantic FieldInfo for additional context
        
    Returns:
        JSON Schema dict
    """
    schema = {}
    
    # Handle Optional types
    origin = get_origin(field_type)
    if origin is type(None) or (hasattr(field_type, '__origin__') and type(None) in get_args(field_type)):
        # Extract the non-None type
        args = get_args(field_type)
        if args:
            field_type = args[0] if args[0] is not type(None) else args[1] if len(args) > 1 else str
        # Re-derive the origin from the unwrapped (non-Optional) type. Without this,
        # Optional[List[...]] / Optional[Dict] keep the outer Union origin and fall
        # through to the scalar branch, emitting {"type": "string"} instead of an
        # array/object schema — which makes OGC Process input validation reject the
        # very arrays the Pydantic model accepts.
        origin = get_origin(field_type)

    # Handle List types (parameterized List[...] or bare list)
    if origin is list or field_type is list:
        args = get_args(field_type)
        item_type = args[0] if args else str
        schema["type"] = "array"
        schema["items"] = _type_to_json_schema(item_type, field_info)
        return schema

    # Handle Dict types (parameterized Dict[...] or bare dict)
    if origin is dict or field_type is dict:
        schema["type"] = "object"
        return schema
    
    # Handle Enum types
    if isinstance(field_type, type) and issubclass(field_type, Enum):
        schema["type"] = "string"
        schema["enum"] = [e.value for e in field_type]
        return schema
    
    # Handle basic types
    type_mapping = {
        str: "string",
        int: "integer",
        float: "number",
        bool: "boolean",
    }
    
    json_type = type_mapping.get(field_type, "string")
    schema["type"] = json_type
    
    # Add default only for optional fields that declare a concrete one. Required
    # fields carry the ``PydanticUndefined`` sentinel (Pydantic v2) — never emit it,
    # or it leaks a non-serializable value into the process description schema.
    if not field_info.is_required():
        if field_info.default is not None and field_info.default is not PydanticUndefined:
            schema["default"] = field_info.default
        elif field_info.default_factory is not None:
            # Call factory to get default value
            try:
                schema["default"] = field_info.default_factory()  # type: ignore[call-arg]
            except Exception:
                pass  # Skip if factory fails

    return schema
