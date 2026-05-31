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

"""Regression guard for the WFS DescribeFeatureType ``data_type -> XSD`` mapping.

Pure unit tests (no DB) over the single canonical mapper that replaced the old
broken two-hop (``data_type -> SQLAlchemy class -> Python-builtin test``), which
collapsed every non-geometry attribute to ``xs:string`` (geoid#1511). The mapper
is keyed on the canonical ``data_type`` vocabulary (the field_types SSOT), so the
DescribeFeatureType XSD now reflects the real column types.
"""

from dynastore.extensions.wfs.tools import canonical_data_type_to_xsd


def test_scalar_types_map_to_correct_xsd():
    assert canonical_data_type_to_xsd("string") == "xs:string"
    assert canonical_data_type_to_xsd("integer") == "xs:int"
    assert canonical_data_type_to_xsd("bigint") == "xs:long"
    assert canonical_data_type_to_xsd("double") == "xs:double"
    assert canonical_data_type_to_xsd("numeric") == "xs:double"
    assert canonical_data_type_to_xsd("boolean") == "xs:boolean"
    assert canonical_data_type_to_xsd("date") == "xs:date"
    assert canonical_data_type_to_xsd("time") == "xs:time"
    assert canonical_data_type_to_xsd("timestamp") == "xs:dateTime"


def test_geometry_maps_to_gml_property_type():
    assert canonical_data_type_to_xsd("geometry") == "gml:GeometryPropertyType"
    # parametrized form (geometry(<type>,<srid>)) must also resolve
    assert canonical_data_type_to_xsd("geometry(Point,4326)") == "gml:GeometryPropertyType"


def test_opaque_and_unknown_types_fall_back_to_string():
    assert canonical_data_type_to_xsd("uuid") == "xs:string"
    assert canonical_data_type_to_xsd("binary") == "xs:string"
    assert canonical_data_type_to_xsd("jsonb") == "xs:string"
    assert canonical_data_type_to_xsd("") == "xs:string"
    assert canonical_data_type_to_xsd("totally-unknown") == "xs:string"


def test_mapping_is_case_insensitive():
    assert canonical_data_type_to_xsd("INTEGER") == "xs:int"
    assert canonical_data_type_to_xsd("Geometry(Point)") == "gml:GeometryPropertyType"
