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

"""TDD tests for Plan Task 6 — converge private builder onto canonical envelope
(refs #1800).

Pins:
  1. ``build_tenant_feature_doc`` produces a canonical envelope shape:
     - ``id`` == geoid (not ``geoid`` at root as a separate key).
     - ``properties`` contains only user attrs (no SYSTEM_FIELD_KEYS).
     - ``stats`` section present when sidecars produce values.
     - ``system`` section present for SYSTEM_FIELD_KEYS values.
     - ``_external_id`` tracker present when external_id supplied.
  2. Private mapping mirrors ``build_item_mapping``'s canonical containers
     (stats/system nested objects; properties typed for declared keys).
  3. Private differs from public ONLY by target index/alias — same
     canonical _source shape.
"""
from __future__ import annotations

from typing import Any, Dict

from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

_SYSTEM_KEYS = frozenset(SYSTEM_FIELD_KEYS)

_GEOID = "019e6318-d99e-7da2-bdd9-test-private"
_EXTERNAL_ID = "EXT-PRIVATE-001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeStatsSidecar:
    def producible_computed_names(self):
        return {"area", "centroid"}

    def resolve_computed_value(self, row, name):
        vals = {"area": 42.0, "centroid": "CAFEBABE"}
        return (True, vals[name]) if name in vals else (False, None)


def _canonical_input_for(geoid: str, external_id: str):
    """Build a CanonicalIndexInput that mirrors what a real PG read returns."""
    from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

    row = {
        "geoid": geoid,
        "external_id": external_id,
        "geometry_hash": "ghash",
        "attributes_hash": "ahash",
        "validity": "[2024-01-01,)",
        "transaction_time": "2026-01-01T00:00:00Z",
        "area": 42.0,
        "centroid": "CAFEBABE",
    }
    return CanonicalIndexInput(
        row=row,
        resolved_sidecars=[_FakeStatsSidecar()],
        geometry={"type": "Point", "coordinates": [0.0, 0.0]},
        bbox=None,
        user_properties={"NAME": "TestPlace"},
        access=None,
    )


# ---------------------------------------------------------------------------
# Task 6a — build_tenant_feature_doc canonical shape
# ---------------------------------------------------------------------------


class TestBuildTenantFeatureDocCanonical:
    """``build_tenant_feature_doc`` must produce the canonical envelope shape,
    not the old flat-geoid + properties-inclusive shape."""

    def test_id_is_geoid_not_separate_geoid_key(self):
        """``id`` in the returned doc must equal the geoid; the old top-level
        ``geoid`` key (distinct from ``id``) must not appear unless it is
        inside ``system``."""
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )

        doc = build_tenant_feature_doc(
            {"id": _GEOID, "properties": {"NAME": "x"}},
            catalog_id="cat",
            collection_id="col",
        )
        assert doc.get("id") == _GEOID, (
            f"doc['id'] must be the geoid; got {doc.get('id')!r}"
        )

    def test_properties_user_only_no_system_keys(self):
        """SYSTEM_FIELD_KEYS must NOT appear in ``properties``."""
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )

        item = {
            "id": _GEOID,
            "properties": {
                "NAME": "x",
                "geoid": _GEOID,       # SYSTEM_FIELD_KEY — must be excluded
                "external_id": "EXT",  # SYSTEM_FIELD_KEY — must be excluded
            },
        }
        doc = build_tenant_feature_doc(
            item, catalog_id="cat", collection_id="col",
        )
        props = doc.get("properties", {})
        for key in SYSTEM_FIELD_KEYS:
            assert key not in props, (
                f"SYSTEM_FIELD_KEY '{key}' leaked into properties: {props}"
            )

    def test_external_id_tracker_present(self):
        """When external_id is supplied, ``_external_id`` tracker must be set."""
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )

        doc = build_tenant_feature_doc(
            {"id": _GEOID, "properties": {}},
            catalog_id="cat",
            collection_id="col",
            external_id=_EXTERNAL_ID,
        )
        assert "_external_id" in doc, (
            f"_external_id tracker missing; doc keys: {list(doc.keys())}"
        )
        assert doc["_external_id"] == _EXTERNAL_ID

    def test_stats_section_populated_from_sidecars(self):
        """When canonical input has sidecars, ``stats`` section must appear."""
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

        ci = _canonical_input_for(_GEOID, _EXTERNAL_ID)
        doc = build_canonical_index_doc(
            ci.row,
            resolved_sidecars=ci.resolved_sidecars,
            known_fields={},
            catalog_id="cat",
            collection_id="col",
            geometry=ci.geometry,
            bbox=ci.bbox,
            user_properties=ci.user_properties,
            access=None,
        )
        assert "stats" in doc, f"stats section missing: {list(doc.keys())}"
        assert doc["stats"].get("area") == 42.0

    def test_system_section_populated_from_row(self):
        """Row SYSTEM_FIELD_KEYS must appear in ``system``, not ``properties``."""
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

        ci = _canonical_input_for(_GEOID, _EXTERNAL_ID)
        doc = build_canonical_index_doc(
            ci.row,
            resolved_sidecars=ci.resolved_sidecars,
            known_fields={},
            catalog_id="cat",
            collection_id="col",
            geometry=ci.geometry,
            bbox=ci.bbox,
            user_properties=ci.user_properties,
            access=None,
        )
        assert "system" in doc, f"system section missing: {list(doc.keys())}"
        # geoid lives in system.
        assert doc["system"].get("geoid") == _GEOID

    def test_private_and_public_produce_same_source_for_same_input(self):
        """Private and public canonical docs must be byte-identical for the
        same canonical input — they differ ONLY by target index/alias."""
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

        ci = _canonical_input_for(_GEOID, _EXTERNAL_ID)
        known_fields: Dict[str, Any] = {"NAME": {"type": "keyword"}}

        # Public path.
        public_doc = build_canonical_index_doc(
            ci.row,
            resolved_sidecars=ci.resolved_sidecars,
            known_fields=known_fields,
            catalog_id="cat",
            collection_id="col",
            geometry=ci.geometry,
            bbox=ci.bbox,
            user_properties=ci.user_properties,
            access=None,
        )

        # Private path — must call build_tenant_feature_doc which should now
        # delegate to build_canonical_index_doc.
        from dynastore.modules.storage.drivers.elasticsearch_private.doc_builder import (
            build_tenant_feature_doc,
        )
        private_doc = build_tenant_feature_doc(
            ci,
            catalog_id="cat",
            collection_id="col",
            known_fields=known_fields,
        )

        assert public_doc == private_doc, (
            f"Private doc diverges from public:\n"
            f"public : {public_doc}\n"
            f"private: {private_doc}\n"
        )


# ---------------------------------------------------------------------------
# Task 6b — private mapping mirrors canonical containers
# ---------------------------------------------------------------------------


class TestPrivateMappingCanonicalContainers:
    """``build_private_item_mapping`` must include canonical ``stats`` and
    ``system`` nested object containers, mirroring ``build_item_mapping``."""

    def test_legacy_mapping_includes_stats_container(self):
        """Legacy (empty overlay) mapping must declare the ``stats`` object."""
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
        )
        props = TENANT_FEATURE_MAPPING.get("properties", {})
        assert "stats" in props, (
            f"TENANT_FEATURE_MAPPING missing 'stats' container; "
            f"present keys: {sorted(props.keys())}"
        )

    def test_legacy_mapping_includes_system_container(self):
        """Legacy (empty overlay) mapping must declare the ``system`` object."""
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
        )
        props = TENANT_FEATURE_MAPPING.get("properties", {})
        assert "system" in props, (
            f"TENANT_FEATURE_MAPPING missing 'system' container; "
            f"present keys: {sorted(props.keys())}"
        )

    def test_legacy_mapping_has_id_keyword_at_root(self):
        """The canonical ``id`` keyword must be present (mirrors
        ``COMMON_PROPERTIES["id"]`` in the public items mapping).
        Private mapping must support both ``id`` (canonical) and ``geoid``
        (backward compat with existing queries)."""
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            TENANT_FEATURE_MAPPING,
        )
        props = TENANT_FEATURE_MAPPING.get("properties", {})
        assert "id" in props, (
            f"TENANT_FEATURE_MAPPING must have 'id' keyword field; "
            f"present keys: {sorted(props.keys())}"
        )
        # geoid must also remain for backward-compat with existing private queries.
        assert "geoid" in props, (
            f"TENANT_FEATURE_MAPPING must still have 'geoid' for backward compat; "
            f"present keys: {sorted(props.keys())}"
        )

    def test_strict_mapping_also_includes_stats_and_system(self):
        """Strict overlay mapping must also carry canonical containers."""
        from dynastore.modules.storage.drivers.elasticsearch_private.mappings import (
            build_private_item_mapping,
        )
        m = build_private_item_mapping({"my:custom": {"type": "keyword"}})
        props = m.get("properties", {})
        assert "stats" in props, (
            f"Strict mapping missing 'stats' container: {sorted(props.keys())}"
        )
        assert "system" in props, (
            f"Strict mapping missing 'system' container: {sorted(props.keys())}"
        )
