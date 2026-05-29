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

"""WFS DescribeFeatureType type mapping.

XSD/GML is WFS-specific, so the ``data_type -> XSD`` table lives here — but it
is keyed on the canonical, GDAL-rooted ``data_type`` vocabulary
(:class:`dynastore.models.field_types.DataType`), the same SSOT that drives
``CANONICAL_TO_PG_DDL`` / ``CANONICAL_TO_JSON_SCHEMA``. Producers emit canonical
``data_type`` directly (validated on ``FieldDefinition``), so this is a single
one-hop lookup — no SQLAlchemy / Python intermediate type juggling.
"""

import logging

logger = logging.getLogger(__name__)


# Canonical ``data_type`` -> XML Schema (XSD) type for WFS DescribeFeatureType /
# GML. Keyed on the canonical tokens (lowercased); ``geometry`` (including the
# parametrized ``geometry(...)`` form) is handled by the prefix check below.
# ``uuid`` / ``binary`` / ``jsonb`` surface as text (UUID string, base64 body,
# opaque JSON) — GML has no finer representation.
_CANONICAL_DATA_TYPE_TO_XSD: dict[str, str] = {
    "string": "xs:string",
    "integer": "xs:int",       # 32-bit
    "bigint": "xs:long",       # 64-bit
    "double": "xs:double",
    "numeric": "xs:double",
    "boolean": "xs:boolean",
    "date": "xs:date",
    "time": "xs:time",
    "timestamp": "xs:dateTime",
    "binary": "xs:string",
    "jsonb": "xs:string",
    "uuid": "xs:string",
}


def canonical_data_type_to_xsd(data_type: str) -> str:
    """Map a canonical ``data_type`` token to its WFS/GML XSD type.

    Geometry (``geometry`` or ``geometry(<type>,<srid>)``) -> the GML property
    type; everything else looks up the canonical table, defaulting to
    ``xs:string`` for any value that bypassed canonicalization.
    """
    pt = (data_type or "").lower()
    if pt.startswith("geometry"):
        return "gml:GeometryPropertyType"
    xsd = _CANONICAL_DATA_TYPE_TO_XSD.get(pt)
    if xsd is None:
        logger.warning("No XSD mapping for data_type %r; using xs:string.", data_type)
        return "xs:string"
    return xsd
