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

"""Unit tests for ``FieldDefinition.parse_format`` (GeoID #1350).

The optional ``parse_format`` hint is a ``strptime`` input pattern that the
ingestion temporal coercion consults to disambiguate numeric date formats
(e.g. day-first ``%d/%m/%Y``). It must round-trip through the config model and
surface in the model's JSON schema (so the configs API exposes it), while
staying ``None`` by default for full backward compatibility with #1333.
"""

from __future__ import annotations

from dynastore.models.protocols.field_definition import FieldDefinition


def test_parse_format_defaults_to_none() -> None:
    fd = FieldDefinition(name="start", data_type="date")
    assert fd.parse_format is None


def test_parse_format_round_trips_through_model() -> None:
    fd = FieldDefinition(name="start", data_type="date", parse_format="%d/%m/%Y")
    assert fd.parse_format == "%d/%m/%Y"
    dumped = fd.model_dump()
    assert dumped["parse_format"] == "%d/%m/%Y"
    assert FieldDefinition(**dumped).parse_format == "%d/%m/%Y"


def test_parse_format_surfaced_in_json_schema() -> None:
    # The configs API exposes the field schema via the model's JSON schema;
    # the new hint must be present there.
    schema = FieldDefinition.model_json_schema()
    assert "parse_format" in schema["properties"]
