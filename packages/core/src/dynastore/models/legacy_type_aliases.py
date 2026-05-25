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
"""TEMPORARY legacy `data_type` aliases — delete once consumers migrate.

The canonical `data_type` vocabulary (``models.field_types.DataType``) is the
strict, GDAL/OGR-rooted SSOT. Some configs authored before that vocabulary
landed still use SQL/legacy spellings (``text``, ``int``, ``float``,
``datetime``, ``json`` …). This module maps those spellings onto canonical
tokens so existing configs keep validating while their authors migrate.

It is deliberately kept **separate** from the canonical vocabulary: the SSOT
enum and ``CANONICAL_DATA_TYPES`` stay alias-free, and
:func:`models.field_types.canonical_data_type` consults this table only as a
fallback before raising. **To retire the compatibility window, delete this
module and the single fallback call in ``canonical_data_type`` — nothing else
depends on it.**

Every alias target here is itself a canonical token, so a normalized result is
always a member of ``CANONICAL_DATA_TYPES``.

These spellings are the "Deprecated" column in ``docs/components/field-types.md``
(e.g. ``real`` -> ``double``); update that doc when this table changes.
"""
from __future__ import annotations

from typing import Dict, Optional

# Legacy / SQL spelling -> canonical ``data_type``. Keys are lowercase; callers
# normalize case before lookup. Date/time/timestamp stay distinct (only the
# zoned/`datetime` spellings fold into ``timestamp``), matching the canonical
# vocabulary's deliberate separation.
LEGACY_DATA_TYPE_ALIASES: Dict[str, str] = {
    # string family
    "text": "string",
    "varchar": "string",
    "char": "string",
    "character": "string",
    "character varying": "string",
    "str": "string",
    "keyword": "string",
    # 32-bit integer family
    "int": "integer",
    "int4": "integer",
    "smallint": "integer",
    "int2": "integer",
    "int32": "integer",
    # 64-bit integer family
    "int8": "bigint",
    "long": "bigint",
    "int64": "bigint",
    # binary floating point
    "float": "double",
    "float8": "double",
    "real": "double",
    "double precision": "double",
    # exact decimal
    "decimal": "numeric",
    "number": "numeric",
    # boolean
    "bool": "boolean",
    # instant (zoned/legacy spellings only — `date` and `time` stay distinct)
    "datetime": "timestamp",
    "timestamptz": "timestamp",
    "timestamp with time zone": "timestamp",
    "timestamp without time zone": "timestamp",
    # json
    "json": "jsonb",
    # uuid
    "guid": "uuid",
    # binary
    "bytea": "binary",
    "blob": "binary",
}


def normalize_legacy_data_type(low: Optional[str]) -> Optional[str]:
    """Map a lowercased legacy/SQL spelling to its canonical token, or ``None``.

    ``low`` must already be stripped + lowercased (the caller in
    ``canonical_data_type`` does this). Returns a canonical ``data_type`` string
    when ``low`` is a known legacy alias, otherwise ``None`` so the caller can
    fall through to its strict-reject path.
    """
    if not low:
        return None
    return LEGACY_DATA_TYPE_ALIASES.get(low)
