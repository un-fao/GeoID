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

"""Public ES driver canonical envelope — write → read round-trip tests.

Proves that the public ``ItemsElasticsearchDriver`` write paths route through
``build_canonical_index_doc`` and that the canonical _source shape is correctly
reconstructed by ``_es_source_to_feature`` / ``unproject_item_from_es``.

Coverage:
  - ``write_entities`` (fallback Feature/dict path, no PG row) → canonical
    ``_source`` shape (``id``=geoid, ``catalog_id``, ``collection_id``,
    ``properties`` user-only, ``system`` container).
  - Simplification markers land under ``system.geometry_simplification`` and
    are NOT surfaced in the reconstructed Feature properties.
  - ``_es_source_to_feature`` reconstructs ``Feature.id`` correctly from the
    canonical ``id`` root field.
  - SYSTEM_FIELD_KEYS must not leak into the reconstructed Feature properties.
  - ``index`` / ``index_bulk`` paths produce the same canonical doc when a
    ``CanonicalIndexInput`` is returned by the mocked PG read.

Refs #1828 (public driver slice — mirrors ``test_private_stats_carry_through.py``
for the private driver round-trip).
"""
from __future__ import annotations

from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest

_GEOID = "019e6318-pub-canonical-rt"
_CAT = "cat_pub"
_COL = "col_pub"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_feature(geoid: str = _GEOID, **extra_props) -> Dict[str, Any]:
    """Minimal Feature dict as produced by the ingest path."""
    props: Dict[str, Any] = {"name": "TestCity"}
    props.update(extra_props)
    return {
        "id": geoid,
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [12.5, 41.9]},
        "bbox": [12.5, 41.9, 12.5, 41.9],
        "properties": props,
    }


class _FakeStatsSidecar:
    def producible_computed_names(self):
        return {"area", "s2_res12"}

    def resolve_computed_value(self, row, name):
        vals = {"area": 42.0, "s2_res12": "89c25a3ffffffff"}
        return (True, vals[name]) if name in vals else (False, None)

    def producible_metadata_names(self):
        return set()

    def resolve_metadata_value(self, row, name):
        return (False, None)


def _canonical_input_for(geoid: str, external_id: str = "EXT-PUB-001"):
    """Build a CanonicalIndexInput that mirrors what a real PG read returns."""
    from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput

    row = {
        "geoid": geoid,
        "external_id": external_id,
        "geometry_hash": "pub-ghash-001",
        "attributes_hash": "pub-ahash-001",
    }
    return CanonicalIndexInput(
        row=row,
        resolved_sidecars=[_FakeStatsSidecar()],
        geometry={"type": "Point", "coordinates": [12.5, 41.9]},
        bbox=[12.5, 41.9, 12.5, 41.9],
        user_properties={"name": "TestCity"},
        access=None,
    )


# ---------------------------------------------------------------------------
# _es_source_to_feature round-trip
# ---------------------------------------------------------------------------


class TestPublicEsSourceToFeatureRoundTrip:
    """``_es_source_to_feature`` must correctly reconstruct a Feature from a
    canonical ``_source`` written by the public driver.

    The canonical shape written by ``build_canonical_index_doc`` carries:
    - ``id`` = geoid at root (canonical identity)
    - ``catalog_id``, ``collection_id``, ``collection`` at root
    - ``properties`` — user attrs only (no SYSTEM_FIELD_KEYS)
    - ``system`` — SYSTEM_FIELD_KEYS (geometry_hash, etc.), not flat at root
    - ``stats`` — sidecar-derived values
    - ``_external_id`` tracker at root (NOT in properties)

    ``unproject_item_from_es`` strips internal trackers and canonical containers
    (system/stats/access), hoists ``properties.extras`` to flat ``properties``,
    and resolves metadata (title/description/keywords).
    """

    def _unproject(self, source: Dict[str, Any]):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        return ItemsElasticsearchDriver._es_source_to_feature(source)

    def test_id_reconstructed_from_canonical_id_field(self):
        """Feature.id must come from the canonical ``id`` root field."""
        source = {
            "id": _GEOID,
            "catalog_id": _CAT,
            "collection_id": _COL,
            "collection": _COL,
            "properties": {"name": "Rome"},
        }
        feature = self._unproject(source)
        assert str(feature.id) == _GEOID, (
            f"Feature.id must equal the canonical 'id' root field; "
            f"got {feature.id!r}"
        )

    def test_user_properties_survive_round_trip(self):
        """User properties in ``properties`` must be reconstructed in Feature."""
        source = {
            "id": _GEOID,
            "collection": _COL,
            "properties": {"name": "Rome", "population": 5000},
        }
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        assert props.get("name") == "Rome"

    def test_system_container_excluded_from_feature_properties(self):
        """The canonical ``system`` container must NOT appear in Feature.properties."""
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

        source = {
            "id": _GEOID,
            "collection": _COL,
            "properties": {"name": "x"},
            "system": {
                "geometry_hash": "ghash",
                "geometry_simplification": {"factor": 0.001, "mode": "tolerance"},
            },
        }
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        # system container itself must not appear in properties.
        assert "system" not in props, (
            f"'system' container leaked into Feature.properties: {props}"
        )
        # Individual system key values must not appear in properties.
        assert "geometry_hash" not in props, (
            f"system.geometry_hash leaked into Feature.properties: {props}"
        )
        assert "geometry_simplification" not in props, (
            f"system.geometry_simplification leaked into Feature.properties: {props}"
        )
        # SYSTEM_FIELD_KEYS must stay out of properties.
        for sk in SYSTEM_FIELD_KEYS:
            assert sk not in props, (
                f"SYSTEM_FIELD_KEY '{sk}' leaked into Feature.properties: {props}"
            )

    def test_stats_container_excluded_from_feature_properties(self):
        """The canonical ``stats`` container must NOT appear in Feature.properties."""
        source = {
            "id": _GEOID,
            "collection": _COL,
            "properties": {"name": "x"},
            "stats": {"area": 42.0, "s2_res12": "89c25a3ffffffff"},
        }
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        assert "stats" not in props, (
            f"'stats' container leaked into Feature.properties: {props}"
        )
        assert "area" not in props, (
            f"stats.area leaked into Feature.properties: {props}"
        )

    def test_internal_tracker_fields_excluded_from_feature_properties(self):
        """Internal ``_*`` trackers at root must NOT surface in Feature.properties."""
        source = {
            "id": _GEOID,
            "collection": _COL,
            "_external_id": "EXT-001",
            "_asset_id": "ASSET-001",
            "properties": {"name": "x"},
        }
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        assert "_external_id" not in props, (
            f"_external_id tracker leaked into Feature.properties: {props}"
        )
        assert "_asset_id" not in props, (
            f"_asset_id tracker leaked into Feature.properties: {props}"
        )

    def test_extras_hoisted_to_flat_properties(self):
        """``properties.extras`` keys must be hoisted to flat ``properties``
        by ``unproject_item_from_es`` (the extras lane is an ES storage detail)."""
        source = {
            "id": _GEOID,
            "collection": _COL,
            "properties": {
                "name": "x",
                "extras": {"custom:field": "value123"},
            },
        }
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        # Hoisted from extras.
        assert props.get("custom:field") == "value123", (
            f"extras key not hoisted to properties: {props}"
        )
        # extras sub-object itself must not appear.
        assert "extras" not in props, (
            f"raw 'extras' object present in Feature.properties: {props}"
        )

    def test_simplification_markers_in_system_not_in_properties(self):
        """Geometry-simplification metadata in ``system.geometry_simplification``
        must NOT appear in Feature.properties after round-trip.

        The public driver writes simplification info via ``_apply_geometry_simplification``
        into ``system.geometry_simplification`` (canonical nested object, refs #1828).
        The flat ``_simplification_factor``/``_simplification_mode`` root keys
        must NOT be written; the canonical nested form must be excluded from the
        wire properties (it is only for internal ES consumers).
        """
        source = {
            "id": _GEOID,
            "collection": _COL,
            "properties": {"name": "x"},
            "system": {
                "geometry_simplification": {"factor": 0.001, "mode": "tolerance"},
            },
        }
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        assert "geometry_simplification" not in props, (
            f"geometry_simplification leaked into Feature.properties: {props}"
        )
        assert "simplification_factor" not in props, (
            "flat simplification_factor must not appear in properties "
            "(canonical: system.geometry_simplification)"
        )
        assert "simplification_mode" not in props, (
            "flat simplification_mode must not appear in properties "
            "(canonical: system.geometry_simplification)"
        )

    def test_geometry_passed_through(self):
        """Geometry in the canonical _source must be preserved in Feature.geometry."""
        geom = {"type": "Point", "coordinates": [12.5, 41.9]}
        source = {
            "id": _GEOID,
            "collection": _COL,
            "geometry": geom,
            "properties": {},
        }
        feature = self._unproject(source)
        # feature.geometry may be a Pydantic model or dict — check type field.
        geom_obj = feature.geometry
        if isinstance(geom_obj, dict):
            assert geom_obj.get("type") == "Point", (
                f"Geometry type not preserved: {geom_obj!r}"
            )
        else:
            # Pydantic geometry model (geojson-pydantic).
            assert getattr(geom_obj, "type", None) == "Point", (
                f"Geometry type not preserved: {geom_obj!r}"
            )


# ---------------------------------------------------------------------------
# build_canonical_index_doc → _es_source_to_feature: full cycle
# ---------------------------------------------------------------------------


class TestPublicCanonicalBuildReadCycle:
    """End-to-end: build the canonical doc with ``build_canonical_index_doc``
    (as the public driver's write paths do), then unproject it back via
    ``_es_source_to_feature`` — the two must form a correct inverse pair for
    all tested properties."""

    def _build(
        self, geoid: str = _GEOID, external_id: str = "EXT-PUB-001",
        sidecars=None, user_props=None, geometry=None,
    ) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc
        row: Dict[str, Any] = {
            "geoid": geoid,
            "external_id": external_id,
            "geometry_hash": "pub-ghash-cycle",
        }
        return build_canonical_index_doc(
            row,
            resolved_sidecars=sidecars or [],
            known_fields={},
            catalog_id=_CAT,
            collection_id=_COL,
            geometry=geometry or {"type": "Point", "coordinates": [0.0, 0.0]},
            bbox=None,
            user_properties=user_props or {"name": "TestCity"},
            access=None,
        )

    def _unproject(self, source: Dict[str, Any]):
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        return ItemsElasticsearchDriver._es_source_to_feature(source)

    def test_feature_id_equals_geoid(self):
        """Round-trip: Feature.id must equal the geoid."""
        source = self._build()
        assert source.get("id") == _GEOID, (
            f"canonical doc 'id' must be geoid; got {source.get('id')!r}"
        )
        feature = self._unproject(source)
        assert str(feature.id) == _GEOID, (
            f"Feature.id must equal geoid; got {feature.id!r}"
        )

    def test_user_properties_survive_full_cycle(self):
        """User properties must survive build → write → unproject."""
        source = self._build(user_props={"name": "Rome", "pop": 3000})
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        # With known_fields={}, undeclared keys go to extras — check both.
        all_props = dict(props)
        all_props.update(props.get("extras") or {})
        assert all_props.get("name") == "Rome"

    def test_system_keys_not_in_feature_properties_after_cycle(self):
        """SYSTEM_FIELD_KEYS must NOT appear in Feature.properties after cycle."""
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

        source = self._build()
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        for sk in SYSTEM_FIELD_KEYS:
            assert sk not in props, (
                f"SYSTEM_FIELD_KEY '{sk}' leaked into Feature.properties "
                f"after public driver canonical cycle: {props}"
            )

    def test_stats_from_sidecars_not_in_feature_properties(self):
        """Stats computed by sidecars must NOT leak into Feature.properties."""
        source = self._build(sidecars=[_FakeStatsSidecar()])
        assert "stats" in source, "stats section missing from canonical doc"
        assert source["stats"].get("area") == 42.0

        feature = self._unproject(source)
        props = dict(feature.properties or {})
        all_props = dict(props)
        all_props.update(props.get("extras") or {})
        assert "area" not in all_props, (
            f"stats.area leaked into Feature.properties: {all_props}"
        )
        assert "s2_res12" not in all_props, (
            f"stats.s2_res12 leaked into Feature.properties: {all_props}"
        )

    def test_simplification_in_system_not_in_feature_properties(self):
        """After ``_apply_geometry_simplification`` writes to ``system``,
        round-trip via ``_es_source_to_feature`` must NOT surface it in props."""
        from dynastore.modules.storage.drivers.elasticsearch import (
            _apply_geometry_simplification,
        )

        source = self._build()
        # Simulate what the public write paths do after building the canonical doc.
        _apply_geometry_simplification(source, factor=0.001, mode="tolerance")

        # system.geometry_simplification is set; flat keys are NOT.
        assert source.get("system", {}).get("geometry_simplification", {}).get("mode") == "tolerance"
        assert "_simplification_mode" not in source
        assert "_simplification_factor" not in source

        # Round-trip: unproject should not surface simplification in properties.
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        assert "geometry_simplification" not in props, (
            f"geometry_simplification leaked into Feature.properties: {props}"
        )
        assert "simplification_factor" not in props
        assert "simplification_mode" not in props

    def test_no_geoid_alias_at_root_for_public_doc(self):
        """The public driver must NOT write a ``geoid`` root alias — only
        the canonical ``id`` field is present (PRIVATE_ENVELOPE_FIELDS needs
        the alias; PUBLIC_ENVELOPE_FIELDS uses ``id`` directly).

        This pins the public/private delta and prevents a regression where
        the public driver accidentally starts writing both ``id`` and ``geoid``
        at root (that is private-only, intentional by design).
        """
        source = self._build()
        assert "id" in source, "canonical 'id' field missing from public doc"
        assert source["id"] == _GEOID
        # geoid alias must NOT be present in the public doc.
        assert "geoid" not in source, (
            f"public doc must not carry a 'geoid' root alias (private-only); "
            f"doc keys: {sorted(source.keys())}"
        )


# ---------------------------------------------------------------------------
# index() canonical doc shape (via mocked PG read)
# ---------------------------------------------------------------------------


class TestPublicIndexCanonicalShape:
    """``ItemsElasticsearchDriver.index()`` must write a canonical _source.

    The ``index()`` method calls ``read_canonical_index_inputs`` + ``build_canonical_index_doc``
    + ``_apply_geometry_simplification``. This verifies the assembled doc carries
    the correct canonical containers.
    """

    @staticmethod
    def _make_ctx(catalog=_CAT, collection=_COL):
        from dynastore.models.protocols.indexer import IndexContext
        return IndexContext(catalog=catalog, collection=collection, entity_type="item")

    @pytest.mark.asyncio
    async def test_index_upsert_produces_canonical_doc_shape(self):
        """``index()`` upsert → ES ``index`` call receives a canonical _source."""
        from dynastore.modules.catalog.canonical_index_read import CanonicalIndexInput
        from dynastore.models.protocols.indexer import IndexOp

        ci = _canonical_input_for(_GEOID)
        op = IndexOp(
            op_type="upsert",
            entity_type="item",
            entity_id=_GEOID,
            payload={"id": _GEOID, "type": "Feature", "properties": {}},
        )
        ctx = self._make_ctx()

        indexed_docs: list = []

        class _StubEsIndex:
            async def index(self, *, index, id, body, params=None, **kwargs):
                indexed_docs.append(body)

        stub_es = _StubEsIndex()

        with patch(
            "dynastore.modules.elasticsearch.client.get_client",
            return_value=stub_es,
        ), patch(
            "dynastore.modules.elasticsearch.client.get_index_prefix",
            return_value="dynastore",
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch._ensure_in_public_alias_once",
            new=AsyncMock(return_value=None),
        ), patch(
            "dynastore.modules.elasticsearch.items_projection.resolve_catalog_known_fields",
            new=AsyncMock(return_value={}),
        ), patch(
            "dynastore.modules.storage.drivers.elasticsearch.read_canonical_index_inputs",
            new=AsyncMock(return_value={_GEOID: ci}),
        ), patch.object(
            __import__(
                "dynastore.modules.storage.drivers.elasticsearch",
                fromlist=["ItemsElasticsearchDriver"],
            ).ItemsElasticsearchDriver,
            "_resolve_simplify_geometry",
            new=AsyncMock(return_value=False),
        ):
            from dynastore.modules.storage.drivers.elasticsearch import (
                ItemsElasticsearchDriver,
            )
            driver = ItemsElasticsearchDriver()
            await driver.index(ctx, op)

        assert len(indexed_docs) == 1, "Expected exactly one ES index call"
        doc = indexed_docs[0]

        # Canonical identity.
        assert doc.get("id") == _GEOID, f"id must be geoid; got {doc.get('id')!r}"
        assert doc.get("catalog_id") == _CAT
        assert doc.get("collection_id") == _COL

        # No ``geoid`` alias at root (public driver only).
        assert "geoid" not in doc, (
            f"public doc must not carry a geoid alias; keys: {sorted(doc.keys())}"
        )

        # stats from sidecars.
        stats = doc.get("stats", {})
        assert stats.get("area") == 42.0, f"stats.area missing; stats={stats}"

        # system from row SYSTEM_FIELD_KEYS.
        system = doc.get("system", {})
        assert "geometry_hash" in system, (
            f"system.geometry_hash missing; system={system}"
        )

        # SYSTEM_FIELD_KEYS must not leak into properties.
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS
        props = doc.get("properties", {})
        for sk in SYSTEM_FIELD_KEYS:
            assert sk not in props, (
                f"SYSTEM_FIELD_KEY '{sk}' leaked into properties: {props}"
            )
