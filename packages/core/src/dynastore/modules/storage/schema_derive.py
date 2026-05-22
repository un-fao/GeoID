"""Derive a Draft-2020-12 JSON-Schema for a feature's ``properties`` from
driver-agnostic ``FieldDefinition`` declarations.

Pure, dependency-light: the collection's items-schema becomes the single source
of truth for both write-validation and the produced feature shape.
"""

from typing import Dict, Literal, Optional

from dynastore.models.field_types import canonical_data_type
from dynastore.models.protocols.field_definition import FieldDefinition

# Canonical data_type (see ``dynastore.models.field_types``) -> JSON-Schema
# property fragment. Keyed on canonical tokens; lookups normalize first.
_TYPE_MAP: Dict[str, dict] = {
    "string": {"type": "string"},
    "uuid": {"type": "string", "format": "uuid"},
    "integer": {"type": "integer"},
    "bigint": {"type": "integer"},
    "double": {"type": "number"},
    "numeric": {"type": "number"},
    "boolean": {"type": "boolean"},
    "timestamp": {"type": "string", "format": "date-time"},
    "date": {"type": "string", "format": "date"},
    "time": {"type": "string", "format": "time"},
    "binary": {"type": "string", "contentEncoding": "base64"},
    "geometry": {"type": "object"},
    "jsonb": {"type": "object"},
}


def derive_wire_schema(
    fields: Dict[str, FieldDefinition],
    *,
    strict: bool = False,
    purpose: Literal["read", "write"] = "read",
) -> Optional[dict]:
    """Build a Draft-2020-12 ``object`` schema for a feature's ``properties``.

    Returns ``None`` when ``fields`` is empty (blob collection — nothing to
    validate).

    ``purpose`` selects which structural constraints the schema carries; in
    both cases the per-field VALUE constraints (type, enum, minimum, maximum,
    maxLength, pattern, format) are always emitted:

    - ``"read"`` (default) — the schema published on the wire and used to
      describe a collection. ``required`` is included, and ``strict=True``
      adds ``additionalProperties: false``.
    - ``"write"`` — the schema fed to the write-path value validator. It
      OMITS both ``required`` and ``additionalProperties`` because those are
      already enforced earlier on the write path by separate mechanisms:
      ``required`` by the ``NOT NULL`` sidecar columns derived from the items
      schema (or, for non-native backends, the ``check_required`` app-level
      fallback), and unknown-key rejection by the strict-unknown-fields
      check. Re-asserting them here would raise a second, conflicting 422 for
      the same input.
    """
    if not fields:
        return None

    include_required = purpose == "read"
    include_additional_properties = purpose == "read"

    properties: Dict[str, dict] = {}
    required = []
    for key, fd in fields.items():
        # Parametrized geometry (e.g. "geometry(Point,4326)") canonicalizes to a
        # lowercased form, so collapse any geometry variant to the base key.
        dt = canonical_data_type(fd.data_type)
        if dt.startswith("geometry"):
            dt = "geometry"
        prop = dict(_TYPE_MAP.get(dt, {"type": "string"}))

        if fd.max_length is not None:
            prop["maxLength"] = fd.max_length
        if fd.minimum is not None:
            prop["minimum"] = fd.minimum
        if fd.maximum is not None:
            prop["maximum"] = fd.maximum
        if fd.enum is not None:
            prop["enum"] = fd.enum
        if fd.pattern is not None:
            prop["pattern"] = fd.pattern
        # Field-level format overrides any date/date-time default.
        if fd.format is not None:
            prop["format"] = fd.format

        properties[key] = prop
        if fd.required is True:
            required.append(key)

    schema: dict = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
    }
    if include_required:
        schema["required"] = sorted(required)
    if strict and include_additional_properties:
        schema["additionalProperties"] = False
    return schema
