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

"""One driver-agnostic projection of the full materialized field set (#1291, #1285 D5).

Every storage driver needs to know the *complete* set of fields a collection
materialises: the author-declared :class:`~dynastore.models.protocols.field_definition.FieldDefinition`
entries from ``ItemsSchema.fields`` PLUS the policy-derived special fields the
``ItemsWritePolicy`` adds (``external_id``, ``asset_id``, content hashes, spatial
cells, geometry/attribute statistics, validity bounds).

Today that derivation is duplicated per driver: only PostgreSQL consumes the
schema, via :func:`...field_constraints.bridge_schema_to_attribute_sidecar` plus a
PG-sidecar-specific policy overlay in the PG driver; Elasticsearch / Iceberg /
DuckDB / BigQuery ignore the schema entirely. :func:`materialize_feature_fields`
is the single source of truth a driver's ``ensure_storage`` can consume instead
of re-deriving the set.

Scope (the safe, behaviour-preserving slice landed first):

* This module IS the single projection contract.
* The PG schema→column decision is shared with this projection via
  :func:`...field_constraints.schema_field_materializes_as_column`, so PG no
  longer carries its own copy of the precedence rule.
* Other drivers are NOT rewired onto this projection here — each driver adopting
  it (so its ``ensure_storage`` consumes ``materialize_feature_fields`` directly)
  is its own follow-up.

The function is pure (no I/O), so a driver can call it once per ``ensure_storage``
with the already-resolved schema + policy and get back a single
``{name: FieldDefinition}`` map describing everything the backing store must hold.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from dynastore.models.protocols.field_definition import (
    FieldCapability,
    FieldDefinition,
)
from dynastore.modules.storage.computed_fields import (
    ComputedKind,
    SPATIAL_CELL_KINDS,
)
from dynastore.modules.storage.field_constraints import (
    schema_field_materializes_as_column,
)

if TYPE_CHECKING:
    from dynastore.modules.storage.computed_fields import ComputedField
    from dynastore.modules.storage.driver_config import ItemsSchema, ItemsWritePolicy


# Canonical ``data_type`` for each computed-field kind. Drivers map the canonical
# token to their own native type (see ``dynastore.models.field_types``). Spatial
# cells and hashes are opaque string keys; statistics are numeric; centroids are
# geometry. Unknown kinds fall back to ``string`` (never raises).
_KIND_DATA_TYPE: "Dict[ComputedKind, str]" = {
    ComputedKind.EXTERNAL_ID: "string",
    ComputedKind.GEOMETRY_HASH: "string",
    ComputedKind.ATTRIBUTES_HASH: "string",
    ComputedKind.GEOHASH: "string",
    ComputedKind.H3: "string",
    ComputedKind.S2: "string",
    ComputedKind.AREA: "double",
    ComputedKind.VOLUME: "double",
    ComputedKind.PERIMETER: "double",
    ComputedKind.LENGTH: "double",
    ComputedKind.CENTROID: "geometry",
    ComputedKind.BBOX: "jsonb",
    ComputedKind.VERTEX_COUNT: "integer",
    ComputedKind.HOLE_COUNT: "integer",
    ComputedKind.CIRCULARITY: "double",
    ComputedKind.CONVEXITY: "double",
    ComputedKind.ASPECT_RATIO: "double",
    ComputedKind.SURFACE_AREA: "double",
    ComputedKind.SURFACE_TO_VOLUME_RATIO: "double",
    ComputedKind.NET_FLOOR_AREA: "double",
    ComputedKind.CENTROID_3D: "geometry",
    ComputedKind.Z_RANGE: "jsonb",
    ComputedKind.VERTICAL_GRADIENT: "double",
    ComputedKind.TEMPORAL_DURATION: "double",
    # ATTRIBUTE_STAT promotes a feature ``properties`` value; its physical type
    # depends on the source, so default to string (the tolerant fallback every
    # driver already uses for a bypassed/unknown type).
    ComputedKind.ATTRIBUTE_STAT: "string",
}

# Special-field names that are document-envelope identifiers, not user data.
# They are not filterable/sortable like ordinary attributes by default; the
# drivers index them with explicit keyword mappings already.
_EXTERNAL_ID_NAME = "external_id"
_ASSET_ID_NAME = "asset_id"
_VALIDITY_START_NAME = "valid_from"
_VALIDITY_END_NAME = "valid_to"


def _computed_field_definition(cf: "ComputedField") -> FieldDefinition:
    """Build a :class:`FieldDefinition` for one engine-facing computed field.

    The output is the cross-driver description of the derived field — its name,
    canonical ``data_type`` and capabilities. A statistic with a storage mode is
    queryable (FILTERABLE/SORTABLE); a non-stored statistic (``storage_mode is
    None``, computed only to feed an identity rule) carries no query capability.
    """
    name = cf.resolved_name
    data_type = _KIND_DATA_TYPE.get(cf.kind, "string")
    caps: list[FieldCapability] = []
    if cf.kind in SPATIAL_CELL_KINDS or cf.kind in (
        ComputedKind.EXTERNAL_ID,
        ComputedKind.GEOMETRY_HASH,
        ComputedKind.ATTRIBUTES_HASH,
    ):
        # Identity-style keys are exact-match queryable.
        caps = [FieldCapability.FILTERABLE]
    elif getattr(cf, "storage_mode", None) is not None:
        # Stored statistic — queryable / orderable.
        caps = [FieldCapability.FILTERABLE, FieldCapability.SORTABLE]
    return FieldDefinition(
        name=name,
        data_type=data_type,
        capabilities=caps,
        # Derived special fields are platform-managed, not author-supplied; keep
        # them out of the public queryables surface unless a driver opts in.
        expose=False,
    )


def materialize_feature_fields(
    schema: "Optional[ItemsSchema]",
    policy: "Optional[ItemsWritePolicy]",
) -> Dict[str, FieldDefinition]:
    """Project the full materialized/queryable field set for a collection.

    Returns ``{name: FieldDefinition}`` covering, in order:

    1. **Schema fields** — every entry in ``schema.fields`` whose ``access`` /
       capabilities / constraints make it a first-class materialised column per
       :func:`...field_constraints.schema_field_materializes_as_column`. Geometry
       is excluded (it is owned by the geometry sidecar / driver). Fields that
       would stay in a JSONB / properties blob are omitted — they are reachable
       but not "materialised" in the column sense this projection describes.
    2. **Policy-derived special fields** — the ``ItemsWritePolicy`` outputs:
       ``external_id`` (when an identity external-id axis exists), ``asset_id``
       (when ``track_asset_id``), content hashes, spatial cells, geometry /
       attribute statistics (via ``policy.compute``), and the validity bounds
       (``valid_from`` / ``valid_to``) when ``policy.validity`` is set.

    A schema field name and a policy-derived name never collide in practice
    (special fields use reserved names); if one did, the explicitly-authored
    schema field wins (it is inserted first and not overwritten).

    Pure and side-effect-free. ``schema`` and/or ``policy`` may be ``None`` (an
    empty / partial config) — each contributes nothing in that case.
    """
    out: Dict[str, FieldDefinition] = {}

    # --- 1. Author-declared schema fields (materialised subset) -------------
    if schema is not None and getattr(schema, "fields", None):
        default_access = getattr(schema, "default_access", None)
        if default_access is None:
            from dynastore.models.protocols.field_definition import FieldAccess
            default_access = FieldAccess.AUTO
        for name, fd in schema.fields.items():
            if not schema_field_materializes_as_column(
                fd, default_access=default_access
            ):
                continue
            # Carry the author's FieldDefinition through unchanged but pin its
            # name to the dict key (schema.fields keys are authoritative).
            out[name] = fd if fd.name == name else fd.model_copy(update={"name": name})

    # --- 2. Policy-derived special fields -----------------------------------
    if policy is not None:
        _project_policy_fields(policy, out)

    return out


def _project_policy_fields(
    policy: "ItemsWritePolicy",
    out: Dict[str, FieldDefinition],
) -> None:
    """Add the ``ItemsWritePolicy``-derived special fields into ``out`` in place."""
    # external_id — the identity external-id axis. ``compute`` carries an
    # EXTERNAL_ID entry only when a source path is set; the axis can also exist
    # path-less. Probe ``find_compute`` first, then fall back to the resolved
    # identity to cover the path-less case.
    has_external_id = policy.find_compute(ComputedKind.EXTERNAL_ID) is not None
    if not has_external_id:
        try:
            resolved = policy.resolved_identity()  # type: ignore[attr-defined]
            for rule in resolved or []:
                if _EXTERNAL_ID_NAME in (getattr(rule, "match_on", None) or []):
                    has_external_id = True
                    break
        except Exception:
            pass
    if has_external_id:
        out.setdefault(
            _EXTERNAL_ID_NAME,
            FieldDefinition(
                name=_EXTERNAL_ID_NAME,
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
                expose=False,
            ),
        )

    # asset_id — present when the policy tracks the source asset reference.
    if getattr(policy, "track_asset_id", False):
        out.setdefault(
            _ASSET_ID_NAME,
            FieldDefinition(
                name=_ASSET_ID_NAME,
                data_type="string",
                capabilities=[FieldCapability.FILTERABLE],
                expose=False,
            ),
        )

    # Content hashes, spatial cells, geometry & attribute statistics — every
    # engine-facing ComputedField except the EXTERNAL_ID axis (handled above).
    for cf in getattr(policy, "compute", None) or []:
        if cf.kind == ComputedKind.EXTERNAL_ID:
            continue
        out.setdefault(cf.resolved_name, _computed_field_definition(cf))

    # Validity bounds — driver-abstracted temporal window. The policy carries
    # only the value sources; the projected fields are the conceptual start/end
    # the drivers expose (PG stores them as a single tstzrange, ES as two dates).
    validity = getattr(policy, "validity", None)
    if validity is not None:
        out.setdefault(
            _VALIDITY_START_NAME,
            FieldDefinition(
                name=_VALIDITY_START_NAME,
                data_type="timestamp",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
                expose=False,
            ),
        )
        out.setdefault(
            _VALIDITY_END_NAME,
            FieldDefinition(
                name=_VALIDITY_END_NAME,
                data_type="timestamp",
                capabilities=[FieldCapability.FILTERABLE, FieldCapability.SORTABLE],
                expose=False,
            ),
        )
