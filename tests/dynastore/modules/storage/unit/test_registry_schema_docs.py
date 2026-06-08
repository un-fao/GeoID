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

"""The registry JSON schema is clean and self-documenting.

The configs API surfaces ``model_json_schema()`` of these config classes (and
their field descriptions through ``_meta``). Two properties must hold for the
operator-/consumer-facing surface:

1. No internal technical references leak — issue numbers (``#1234``) and
   internal cross-reference link markers belong in code comments, never in the
   schema descriptions an external consumer reads.
2. The type-bearing fields enumerate their allowed values, so a consumer can
   tell *what to write* without reading the source.
"""
from __future__ import annotations

import json
import re

import pytest

from dynastore.models.field_types import CANONICAL_DATA_TYPES, GDAL_BAND_TYPES
from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.gdal.models import Band, RasterInfo
from dynastore.modules.storage.driver_config import ItemsSchema, ItemsWritePolicy
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    FeatureAttributeSidecarConfig,
)
from dynastore.modules.storage.read_policy import ItemsReadPolicy

# ``#/$defs`` JSON pointers are not issue refs (no digit after ``#``); ``[[`` in
# examples (list-of-lists) is not a memory link (no letter after ``[[``).
_ISSUE_REF = re.compile(r"#\d{2,4}\b")
_MEMORY_LINK = re.compile(r"\[\[[A-Za-z]")

_REGISTRY_MODELS = [
    ItemsSchema, ItemsWritePolicy, ItemsReadPolicy,
    AttributeSchemaEntry, FeatureAttributeSidecarConfig,
    FieldDefinition, Band, RasterInfo,
]


@pytest.mark.parametrize("model", _REGISTRY_MODELS, ids=lambda m: m.__name__)
def test_schema_has_no_issue_references(model) -> None:
    schema = json.dumps(model.model_json_schema())
    leaked = sorted(set(_ISSUE_REF.findall(schema)))
    assert not leaked, f"{model.__name__} json_schema leaks issue refs: {leaked}"


@pytest.mark.parametrize("model", _REGISTRY_MODELS, ids=lambda m: m.__name__)
def test_schema_has_no_memory_links(model) -> None:
    schema = json.dumps(model.model_json_schema())
    assert not _MEMORY_LINK.search(schema), (
        f"{model.__name__} json_schema leaks an internal cross-reference marker "
        "into user-facing docs"
    )


def test_data_type_enumerates_allowed_values() -> None:
    props = FieldDefinition.model_json_schema()["properties"]
    desc = props["data_type"]["description"]
    # every canonical value is discoverable from the description
    for token in CANONICAL_DATA_TYPES:
        assert token in desc, f"data_type description omits {token!r}"
    assert "geometry(Point,4326)" in props["data_type"]["examples"]


def test_band_type_enumerates_allowed_values() -> None:
    desc = Band.model_json_schema()["properties"]["type"]["description"]
    for token in GDAL_BAND_TYPES:
        assert token in desc, f"Band.type description omits {token!r}"
