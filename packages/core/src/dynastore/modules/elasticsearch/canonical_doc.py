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

"""Canonical, policy-independent Elasticsearch _source builder for items.

Phase 1 of the canonical-index-envelope initiative (#1800): additive write-side
builder only. No wiring into write paths, no reindex, no behavior change.
"""
from typing import Any, Dict, List, Optional

from dynastore.modules.elasticsearch.items_projection import project_item_for_es
from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

_SYSTEM_KEYS: frozenset = frozenset(SYSTEM_FIELD_KEYS)


def build_canonical_index_doc(
    row: Dict[str, Any],
    *,
    resolved_sidecars: List[Any],
    known_fields: Dict[str, Any],
    catalog_id: str,
    collection_id: str,
    geometry: Optional[dict] = None,
    bbox: Optional[list] = None,
    user_properties: Optional[Dict[str, Any]] = None,
    access: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the policy-independent canonical ES _source.

    Sections:
      - flat identity: id(=geoid), catalog_id, collection_id, external_id,
        asset_id, validity
      - geometry, bbox
      - properties: user attrs, typed-known kept flat, unknown moved to
        properties.extras (via project_item_for_es — same reshape as the
        rest of the ES projection path)
      - stats: every producible computed value from resolved_sidecars NOT in
        SYSTEM_FIELD_KEYS (system wins the overlap)
      - system: SYSTEM_FIELD_KEYS values present on the row (content hashes
        belong here)
      - access: pass-through when non-empty
      - _external_id: transition tracker mirrored at top level (read path
        depends on it)

    ``id`` is ALWAYS ``row["geoid"]``, regardless of any policy.
    Reserved STAC/GeoJSON members never leak into properties.
    """
    geoid = row.get("geoid")
    doc: Dict[str, Any] = {
        "id": geoid,
        "catalog_id": catalog_id,
        "collection_id": collection_id,
        # ``collection`` is the STAC/GeoJSON wire member (a reserved key the
        # read reconstruction surfaces verbatim); ``collection_id`` is the
        # internal queryable/filter field. Both are carried until the
        # read-time projector derives the wire ``collection`` from
        # ``collection_id`` (#1285). Dropping it here would null the
        # ``collection`` member on every ES-served STAC hit and break the
        # ``collection``-term existence check used by the REFUSE write policy.
        "collection": collection_id,
    }

    external_id = row.get("external_id")
    if external_id is not None:
        doc["external_id"] = str(external_id)
        doc["_external_id"] = str(external_id)   # transition tracker (read path)

    if row.get("asset_id") is not None:
        doc["asset_id"] = str(row["asset_id"])

    if row.get("validity") is not None:
        doc["validity"] = row["validity"]

    if geometry is not None:
        doc["geometry"] = geometry

    if bbox is not None:
        doc["bbox"] = bbox

    # properties: user attrs only; unknown keys are moved to properties.extras
    # by reusing the existing projection function so behavior matches the rest
    # of the ES projection path.
    doc["properties"] = dict(user_properties or {})
    doc = project_item_for_es(doc, known_fields)

    # system: SYSTEM_FIELD_KEYS values present on the row (content hashes live
    # here, not in stats).
    system = {k: row[k] for k in SYSTEM_FIELD_KEYS if row.get(k) is not None}
    if system:
        doc["system"] = system

    # stats: producible computed values from sidecars NOT claimed by system.
    # System wins all overlaps: if a sidecar also produces geometry_hash it
    # still lives in system only.
    stats: Dict[str, Any] = {}
    for sidecar in resolved_sidecars:
        for name in sidecar.producible_computed_names():
            if name in _SYSTEM_KEYS or name in stats:
                continue
            found, value = sidecar.resolve_computed_value(row, name)
            if found and value is not None:
                stats[name] = value
    if stats:
        doc["stats"] = stats

    if access:
        doc["access"] = access

    return doc


__all__ = ["build_canonical_index_doc"]
