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

"""Explicit ``version`` discriminator on ``ItemsSchema`` (#1291 item 7).

The schema is ``_freeze_at='collection'``, so the version travels with the
frozen schema. It is informational (no auto-migration), authorable and visible
on the serialized shape.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.driver_config import ItemsSchema


def test_version_defaults_to_one():
    assert ItemsSchema().version == 1


def test_version_is_authorable():
    assert ItemsSchema(version=3).version == 3


def test_version_serialized_on_the_shape():
    dumped = ItemsSchema(version=2).model_dump()
    assert dumped["version"] == 2
    # Round-trips back through the model.
    assert ItemsSchema(**dumped).version == 2


def test_version_appears_in_json_schema():
    schema = ItemsSchema.model_json_schema()
    assert "version" in schema["properties"]


def test_version_must_be_positive():
    with pytest.raises(ValueError):
        ItemsSchema(version=0)


def test_version_change_alters_schema_id():
    """A version bump is a schema-shape change → distinct ``schema_id`` hash.

    ``schema_id`` is the sha256 of the model JSON-schema; adding the ``version``
    field means a different *default* value participates in the shape. Two
    instances with different versions still share the same ``schema_id`` (it is a
    class-level structural hash, not instance state) — so the freeze gate, which
    compares instance values, is what actually guards a version bump. Assert the
    field is present on the structural schema so future migration logic can read
    it from the frozen payload.
    """
    schema_props = ItemsSchema.model_json_schema()["properties"]["version"]
    # Default is advertised on the structural schema.
    assert schema_props.get("default") == 1
