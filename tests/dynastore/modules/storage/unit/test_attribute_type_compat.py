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
"""The PG attribute ``type`` accepts both vocabularies (#1216).

``AttributeSchemaEntry.type`` is a ``PostgresType`` (physical PG-DDL names).
Its before-validator additionally accepts the canonical ``data_type`` vocabulary
and the temporary legacy aliases, mapping them to one ``PostgresType`` — so a
config written against the new mapping AND a pre-canonical config (whose
``attribute_schema`` uses physical names like ``TEXT``/``INTEGER``) both keep
validating.
"""
from __future__ import annotations

import pytest

from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeSchemaEntry,
    PostgresType,
)


@pytest.mark.parametrize("value,expected", [
    # native PG-DDL names (old) — taken as-is, any case
    ("TEXT", PostgresType.TEXT),
    ("INTEGER", PostgresType.INTEGER),
    ("text", PostgresType.TEXT),
    ("VARCHAR(255)", PostgresType.VARCHAR_255),
    # canonical data_type tokens (new) — bridged to PG-DDL
    ("string", PostgresType.TEXT),
    ("integer", PostgresType.INTEGER),
    ("bigint", PostgresType.BIGINT),
    ("double", PostgresType.FLOAT),
    ("numeric", PostgresType.NUMERIC),
    ("boolean", PostgresType.BOOLEAN),
    ("timestamp", PostgresType.TIMESTAMPTZ),
    ("jsonb", PostgresType.JSONB),
    # temporary legacy aliases — normalized through the canonical layer
    ("int", PostgresType.INTEGER),
    ("float", PostgresType.FLOAT),
    ("datetime", PostgresType.TIMESTAMPTZ),
    ("json", PostgresType.JSONB),
    ("bool", PostgresType.BOOLEAN),
])
def test_attribute_type_accepts_both_vocabularies(value, expected) -> None:
    assert AttributeSchemaEntry(name="c", type=value).type is expected


def test_native_postgrestype_instance_passes_through() -> None:
    e = AttributeSchemaEntry(name="c", type=PostgresType.BIGINT)
    assert e.type is PostgresType.BIGINT


def test_default_is_text() -> None:
    assert AttributeSchemaEntry(name="c").type is PostgresType.TEXT


def test_unknown_type_still_rejected() -> None:
    with pytest.raises(ValueError):
        AttributeSchemaEntry(name="c", type="frobnicate")
