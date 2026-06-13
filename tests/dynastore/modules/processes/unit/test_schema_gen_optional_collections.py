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

"""``pydantic_to_process_inputs`` — Optional collection and required-field handling.

Two regressions in the OGC Process input-schema generator:

1. ``Optional[List[X]]`` / ``Optional[Dict]`` collapsed to ``{"type": "string"}``.
   After unwrapping the ``Optional`` (``Union[..., None]``) the helper kept the
   *outer* ``Union`` origin, so the ``origin is list`` / ``origin is dict`` checks
   missed and it fell through to the scalar branch. The async DWH-join Process
   then rejected the very arrays (``properties``/``stats``/``system``) that the
   synchronous endpoint accepted, with ``422 ... ['*'] is not of type 'string'``.

2. Required fields (Pydantic v2 ``Field(...)``) leaked the ``PydanticUndefined``
   sentinel into ``schema["default"]`` — the stale ``!= ...`` guard was v1-style.
"""

import json
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from dynastore.modules.processes.schema_gen import pydantic_to_process_inputs


class _Sample(BaseModel):
    required_str: str = Field(..., description="A required string")
    opt_list: Optional[List[str]] = Field(None, description="Optional list of names")
    opt_dict: Optional[dict] = Field(None, description="Optional bare dict")
    opt_typed_dict: Optional[Dict[str, int]] = None
    opt_int: Optional[int] = None
    flag: bool = Field(default=True, description="A boolean with a default")


def test_optional_list_becomes_array_of_items():
    schema = pydantic_to_process_inputs(_Sample)["opt_list"]["schema"]
    assert schema == {"type": "array", "items": {"type": "string"}}


def test_optional_bare_dict_becomes_object():
    schema = pydantic_to_process_inputs(_Sample)["opt_dict"]["schema"]
    assert schema == {"type": "object"}


def test_optional_typed_dict_becomes_object():
    schema = pydantic_to_process_inputs(_Sample)["opt_typed_dict"]["schema"]
    assert schema == {"type": "object"}


def test_optional_scalar_unwraps_to_concrete_type():
    schema = pydantic_to_process_inputs(_Sample)["opt_int"]["schema"]
    assert schema["type"] == "integer"


def test_required_field_has_no_default_and_no_undefined_leak():
    schema = pydantic_to_process_inputs(_Sample)["required_str"]["schema"]
    assert schema["type"] == "string"
    assert "default" not in schema


def test_optional_with_default_keeps_default():
    schema = pydantic_to_process_inputs(_Sample)["flag"]["schema"]
    assert schema == {"type": "boolean", "default": True}


def test_full_inputs_are_json_serializable():
    # PydanticUndefined is not JSON-serializable; a leak would raise here.
    serialized = json.dumps(pydantic_to_process_inputs(_Sample))
    assert "Undefined" not in serialized
