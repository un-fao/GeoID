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

"""Pins the WFS DescribeFeatureType ``data_type -> XSD`` mapping behaviour.

These are pure unit tests (no DB) over the WFS type-mapping helpers. They
document a *known* defect tracked in geoid#1511: ``introspect_feature_type_schema``
composes ``_map_protocol_type_to_sqlalchemy`` (which returns SQLAlchemy type
*classes*) with ``get_xsd_type_from_python_type`` (which tests Python *builtins*).
The two never match, so every non-geometry attribute collapses to ``xs:string``.

The correct SQLAlchemy mapper (``get_xsd_type_from_sa_type``) already exists and
returns the right XSD for SQLAlchemy *instances* — it is simply not wired in.

When the #1511 type-fidelity fix lands (single canonical ``data_type -> XSD``
map keyed off the field_types SSOT), the "currently buggy" assertions below
flip to the correct XSD types, and this file becomes the regression guard for
the change against the field-tested QGIS DescribeFeatureType surface.
"""

from sqlalchemy import Boolean, Float, Integer, TIMESTAMP

from dynastore.extensions.wfs.tools import (
    get_xsd_type_from_python_type,
    get_xsd_type_from_sa_type,
)
from dynastore.extensions.wfs.wfs_db import _map_protocol_type_to_sqlalchemy


# --- The correct SQLAlchemy -> XSD mapper (currently UNWIRED) ----------------


def test_sa_mapper_yields_correct_xsd_for_instances():
    # get_xsd_type_from_sa_type is correct — it just needs SQLAlchemy instances.
    assert get_xsd_type_from_sa_type(Integer()) == "xs:long"
    assert get_xsd_type_from_sa_type(Float()) == "xs:double"
    assert get_xsd_type_from_sa_type(Boolean()) == "xs:boolean"
    assert get_xsd_type_from_sa_type(TIMESTAMP()) == "xs:dateTime"


# --- The currently-wired composition (DOCUMENTS THE #1511 DEFECT) ------------


def _wired_path(canonical_data_type: str) -> str:
    """Exactly what ``introspect_feature_type_schema`` does today for a
    non-geometry field: SQLAlchemy *class* -> Python-builtin mapper."""
    return get_xsd_type_from_python_type(
        _map_protocol_type_to_sqlalchemy(canonical_data_type)
    )


def test_protocol_mapper_returns_sqlalchemy_classes_not_instances():
    # The mismatch source: classes, not instances / builtins.
    assert _map_protocol_type_to_sqlalchemy("integer") is Integer
    assert _map_protocol_type_to_sqlalchemy("double") is Float
    assert _map_protocol_type_to_sqlalchemy("boolean") is Boolean


def test_wired_path_currently_collapses_every_type_to_string():
    # KNOWN DEFECT (geoid#1511): non-geometry attributes all surface as
    # xs:string regardless of their real type. Flip these once the fix lands.
    for canonical in ("integer", "double", "boolean", "timestamp", "date"):
        assert _wired_path(canonical) == "xs:string"
