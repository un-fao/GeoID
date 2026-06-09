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

"""Unit tests for STAC fidelity in default (no-schema/no-StacPreset) catalogs.

Bug context: ingesting a STAC item with colon-namespaced extension properties
(``proj:bbox``, ``proj:code``, ``proj:geometry``, ``cube:dimensions``,
``cube:variables``) into a bare catalog silently dropped all such keys. The
surviving keys were exactly the non-namespaced ones — the colon in the key name
was the bug signature.

Root cause: ``ItemMetadataSidecar.prepare_upsert_payload`` (ordered PRUNE_FIRST)
calls ``prune_stac_managed_properties(feature["properties"])`` which
strips ALL colon-namespaced keys from the live dict IN PLACE.
``FeatureAttributeSidecar.prepare_upsert_payload`` (JSONB mode) then reads from
the already-pruned dict — all extension keys are gone.

Fix: JSONB mode now reads from ``context["_pristine_item"]``, a deep copy taken
before any sidecar runs, so all extension keys are preserved.
Top-level STAC reserved members (``assets``, ``stac_extensions``) are also folded
into the JSONB blob so the stac_metadata sidecar absence is transparent.

These tests cover the surface verifiable without a live DB:

* (a) JSONB write with ``_pristine_item`` context preserves colon-namespaced
  properties (``proj:bbox``, ``cube:dimensions``) even when the live properties
  dict has been pruned of those keys.
* (b) Top-level ``assets`` and ``stac_extensions`` are folded into the JSONB
  payload when absent from the live properties dict.
* (c) canonical doc + unproject round-trip restores ``proj:*``/``cube:*``,
  ``stac_extensions``, and ``assets`` for a default catalog (no stac_metadata
  sidecar — JSONB-only path).
* (d) COLUMNAR mode is unaffected: ``_pristine_item`` path is exclusive to JSONB.
"""
from __future__ import annotations

import copy
import json
from typing import Any, Dict, Optional

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    AttributeStorageMode,
    FeatureAttributeSidecarConfig,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_GEOID = "019f0001-stac-fidelity-unit"
_CAT = "cat_default"
_COL = "col_default"

_STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
    "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
]

_ASSETS = {
    "data": {
        "href": "https://example.com/data.tif",
        "type": "image/tiff",
        "roles": ["data"],
    }
}

_FULL_PROPERTIES: Dict[str, Any] = {
    "datetime": "2024-01-15T00:00:00Z",
    "title": "Test STAC Item",
    # STAC projection extension properties — colon-namespaced
    "proj:bbox": [10.0, 40.0, 20.0, 50.0],
    "proj:code": "EPSG:4326",
    "proj:geometry": {
        "type": "Polygon",
        "coordinates": [[[10.0, 40.0], [20.0, 40.0], [20.0, 50.0], [10.0, 50.0], [10.0, 40.0]]],
    },
    # STAC datacube extension properties — colon-namespaced
    "cube:dimensions": {
        "x": {"type": "spatial", "axis": "x"},
        "y": {"type": "spatial", "axis": "y"},
    },
    "cube:variables": {
        "ndvi": {"type": "data", "unit": ""},
    },
}


def _make_raw_item(properties: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Full STAC feature dict as supplied by the ingest caller."""
    return {
        "id": "test-item-001",
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [15.0, 45.0]},
        "bbox": [15.0, 45.0, 15.0, 45.0],
        "properties": dict(properties or _FULL_PROPERTIES),
        "stac_version": "1.0.0",
        "stac_extensions": list(_STAC_EXTENSIONS),
        "assets": dict(_ASSETS),
        "collection": _COL,
    }


def _make_pruned_properties() -> Dict[str, Any]:
    """Simulate the post-prune state of properties after ItemMetadataSidecar
    strips all colon-namespaced keys in place (the bug state)."""
    return {k: v for k, v in _FULL_PROPERTIES.items() if ":" not in k}


def _jsonb_sidecar() -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.JSONB)
    )


def _columnar_sidecar() -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(
        FeatureAttributeSidecarConfig(storage_mode=AttributeStorageMode.COLUMNAR)
    )


def _decode_attrs(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Decode the JSONB payload key, which is stored as a JSON string by
    ``prepare_upsert_payload`` (asyncpg JSONB codec requirement)."""
    raw = payload.get("attributes")
    if raw is None:
        return {}
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    return {}


def _minimal_context(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Build a write context as item_service.upsert provides it.

    The key invariant: ``_pristine_item`` is set to a deep copy of the
    original feature BEFORE any sidecar runs (item_service.py:1328).
    The live feature dict may already be mutated (properties pruned) when the
    attributes sidecar runs.
    """
    return {
        "geoid": _GEOID,
        "_pristine_item": copy.deepcopy(raw_item),
    }


# ---------------------------------------------------------------------------
# (a) JSONB mode: colon-namespaced properties survive prune via _pristine_item
# ---------------------------------------------------------------------------


class TestJsonbPreservesColonNamespacedProps:
    """JSONB sidecar must read from ``_pristine_item`` and carry all
    colon-namespaced extension keys, even when the live properties dict has
    already been stripped by the PRUNE_FIRST ``ItemMetadataSidecar``."""

    def _payload_from_pruned_live(self) -> Dict[str, Any]:
        """Return the attributes payload after simulating the pruned live state."""
        raw_item = _make_raw_item()
        ctx = _minimal_context(raw_item)

        # Simulate item_metadata PRUNE_FIRST in-place mutation
        raw_item["properties"] = _make_pruned_properties()

        sidecar = _jsonb_sidecar()
        payload = sidecar.prepare_upsert_payload(raw_item, ctx)
        assert payload is not None, "prepare_upsert_payload returned None"
        return payload

    def test_proj_bbox_survives_prune(self):
        payload = self._payload_from_pruned_live()
        attrs = _decode_attrs(payload)
        assert "proj:bbox" in attrs, (
            f"proj:bbox missing from JSONB payload after prune; attrs={sorted(attrs.keys())}"
        )

    def test_proj_code_survives_prune(self):
        payload = self._payload_from_pruned_live()
        attrs = _decode_attrs(payload)
        assert "proj:code" in attrs, (
            f"proj:code missing from JSONB payload after prune; attrs={sorted(attrs.keys())}"
        )

    def test_proj_geometry_survives_prune(self):
        payload = self._payload_from_pruned_live()
        attrs = _decode_attrs(payload)
        assert "proj:geometry" in attrs, (
            f"proj:geometry missing from JSONB payload after prune; attrs={sorted(attrs.keys())}"
        )

    def test_cube_dimensions_survives_prune(self):
        payload = self._payload_from_pruned_live()
        attrs = _decode_attrs(payload)
        assert "cube:dimensions" in attrs, (
            f"cube:dimensions missing from JSONB payload after prune; attrs={sorted(attrs.keys())}"
        )

    def test_cube_variables_survives_prune(self):
        payload = self._payload_from_pruned_live()
        attrs = _decode_attrs(payload)
        assert "cube:variables" in attrs, (
            f"cube:variables missing from JSONB payload after prune; attrs={sorted(attrs.keys())}"
        )

    def test_non_namespaced_props_also_present(self):
        """Non-namespaced props must also survive (regression guard)."""
        payload = self._payload_from_pruned_live()
        attrs = _decode_attrs(payload)
        assert "datetime" in attrs, (
            f"datetime missing from JSONB payload; attrs={sorted(attrs.keys())}"
        )
        assert "title" in attrs, (
            f"title missing from JSONB payload; attrs={sorted(attrs.keys())}"
        )

    def test_no_pristine_falls_back_to_live_dict(self):
        """When ``_pristine_item`` is absent the sidecar falls back to the live
        properties dict (legacy path for callers that do not set the key)."""
        raw_item = _make_raw_item()
        # Context WITHOUT _pristine_item
        ctx = {"geoid": _GEOID}
        sidecar = _jsonb_sidecar()
        payload = sidecar.prepare_upsert_payload(raw_item, ctx)
        assert payload is not None
        attrs = _decode_attrs(payload)
        # The live properties still carry all keys (no prune happened yet here)
        assert "proj:bbox" in attrs, (
            f"proj:bbox missing without pristine; attrs={sorted(attrs.keys())}"
        )


# ---------------------------------------------------------------------------
# (b) JSONB mode: top-level assets / stac_extensions folded in
# ---------------------------------------------------------------------------


class TestJsonbFoldsTopLevelStacMembers:
    """Top-level ``assets`` and ``stac_extensions`` from the original feature
    must be folded into the JSONB payload when they are absent from the live
    properties dict (they are in ``_RESERVED_MEMBER_KEYS`` and therefore
    stripped by ``project_item_for_es`` / ``strip_reserved_members`` before
    any ES write — without this fold they would have no storage path)."""

    def _payload(self) -> Dict[str, Any]:
        raw_item = _make_raw_item()
        ctx = _minimal_context(raw_item)
        # Simulate full prune: live properties has no STAC members at all
        raw_item["properties"] = _make_pruned_properties()
        sidecar = _jsonb_sidecar()
        payload = sidecar.prepare_upsert_payload(raw_item, ctx)
        assert payload is not None
        return payload

    def test_stac_extensions_folded_into_jsonb(self):
        payload = self._payload()
        attrs = _decode_attrs(payload)
        assert "stac_extensions" in attrs, (
            f"stac_extensions not folded into JSONB blob; attrs={sorted(attrs.keys())}"
        )
        assert attrs["stac_extensions"] == _STAC_EXTENSIONS

    def test_assets_folded_into_jsonb(self):
        payload = self._payload()
        attrs = _decode_attrs(payload)
        assert "assets" in attrs, (
            f"assets not folded into JSONB blob; attrs={sorted(attrs.keys())}"
        )
        data_asset = attrs["assets"].get("data", {})
        assert data_asset.get("href") == "https://example.com/data.tif"

    def test_assets_already_in_pristine_properties_not_overwritten_by_fold(self):
        """When assets already appear in the PRISTINE properties dict (i.e. the
        original feature had 'assets' as a property key alongside the top-level
        'assets'), the fold must not overwrite the properties version with the
        top-level one.  Properties-level value wins over the top-level fold."""
        # Build a feature where the original properties dict ALREADY contains
        # an "assets" key (e.g. a denormalised copy placed there by the caller).
        props_with_assets = dict(_FULL_PROPERTIES)
        props_with_assets["assets"] = {"thumbnail": {"href": "https://example.com/thumb.png"}}
        raw_item = _make_raw_item(properties=props_with_assets)
        # Pristine copy taken now — pristine.properties DOES contain "assets"
        ctx = _minimal_context(raw_item)

        sidecar = _jsonb_sidecar()
        payload = sidecar.prepare_upsert_payload(raw_item, ctx)
        assert payload is not None
        attrs = _decode_attrs(payload)
        # "assets" comes from pristine.properties, not from the top-level fold
        assert "assets" in attrs
        # The properties-level value (thumbnail) must win, not the top-level one (data)
        assert "thumbnail" in attrs["assets"], (
            f"Properties-level assets must not be overwritten by top-level fold; "
            f"assets={attrs.get('assets')}"
        )
        assert "data" not in attrs["assets"], (
            "Top-level 'data' asset must not replace the properties-level 'thumbnail' "
            "when pristine.properties already contained 'assets'"
        )


# ---------------------------------------------------------------------------
# (c) Canonical doc + unproject round-trip for default catalog
# ---------------------------------------------------------------------------


class TestCanonicalRoundTripDefaultCatalog:
    """End-to-end: simulate the ES write → read cycle for a default-catalog item.

    In the default (no-schema, JSONB) path the JSONB blob carries all
    properties including colon-namespaced extension keys, ``assets``, and
    ``stac_extensions``.  The canonical index doc builder receives these via
    ``stac_reserved_members`` (for ``assets``/``stac_extensions``) and
    ``user_properties`` (for colon-namespaced props that land in
    ``properties.extras``).  ``_es_source_to_feature`` must then reconstruct
    the full set of properties from the canonical doc."""

    def _build_canonical(
        self,
        user_props: Dict[str, Any],
        stac_reserved: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        from dynastore.modules.elasticsearch.canonical_doc import build_canonical_index_doc

        row: Dict[str, Any] = {
            "geoid": _GEOID,
            "external_id": "test-item-001",
            "geometry_hash": "ghash-test",
        }
        return build_canonical_index_doc(
            row,
            resolved_sidecars=[],
            known_fields={},
            catalog_id=_CAT,
            collection_id=_COL,
            geometry={"type": "Point", "coordinates": [15.0, 45.0]},
            bbox=[15.0, 45.0, 15.0, 45.0],
            user_properties=user_props,
            access=None,
            stac_reserved_members=stac_reserved,
        )

    def _unproject(self, source: Dict[str, Any]) -> Dict[str, Any]:
        from dynastore.modules.storage.drivers.elasticsearch import (
            ItemsElasticsearchDriver,
        )
        return ItemsElasticsearchDriver._es_source_to_feature(source)

    def test_proj_bbox_survives_canonical_round_trip(self):
        """``proj:bbox`` stored in user_properties as extras survives round-trip."""
        user_props = dict(_FULL_PROPERTIES)
        source = self._build_canonical(user_props)
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        # Extras are hoisted back to flat properties by unproject
        extras = props.pop("extras", {}) or {}
        all_props = {**props, **extras}
        assert "proj:bbox" in all_props, (
            f"proj:bbox missing after round-trip; props={sorted(all_props.keys())}"
        )
        assert all_props["proj:bbox"] == [10.0, 40.0, 20.0, 50.0]

    def test_cube_dimensions_survives_canonical_round_trip(self):
        """``cube:dimensions`` survives build → unproject cycle."""
        user_props = dict(_FULL_PROPERTIES)
        source = self._build_canonical(user_props)
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        extras = props.pop("extras", {}) or {}
        all_props = {**props, **extras}
        assert "cube:dimensions" in all_props, (
            f"cube:dimensions missing after round-trip; props={sorted(all_props.keys())}"
        )

    def test_stac_extensions_restored_at_top_level(self):
        """``stac_extensions`` passed via stac_reserved_members lands at the
        top level of the canonical doc and is surfaced by unproject."""
        user_props = {k: v for k, v in _FULL_PROPERTIES.items()}
        stac_rsv = {"stac_extensions": list(_STAC_EXTENSIONS)}
        source = self._build_canonical(user_props, stac_reserved=stac_rsv)

        # Top-level in canonical doc
        assert source.get("stac_extensions") == _STAC_EXTENSIONS, (
            f"stac_extensions not at doc root; keys={sorted(source.keys())}"
        )

        # Survives unproject
        feature = self._unproject(source)
        assert hasattr(feature, "stac_extensions") or "stac_extensions" in (feature if isinstance(feature, dict) else {}), (
            "stac_extensions missing from unprojected feature"
        )
        if isinstance(feature, dict):
            assert feature.get("stac_extensions") == _STAC_EXTENSIONS
        else:
            assert getattr(feature, "stac_extensions", None) == _STAC_EXTENSIONS

    def test_assets_restored_at_top_level(self):
        """``assets`` passed via stac_reserved_members lands at the top level
        and is surfaced by unproject."""
        user_props = {k: v for k, v in _FULL_PROPERTIES.items()}
        stac_rsv = {"assets": dict(_ASSETS)}
        source = self._build_canonical(user_props, stac_reserved=stac_rsv)

        # Top-level in canonical doc
        assert "assets" in source, (
            f"assets not at doc root; keys={sorted(source.keys())}"
        )

        # Survives unproject
        feature = self._unproject(source)
        if isinstance(feature, dict):
            assert "assets" in feature
            assert feature["assets"].get("data", {}).get("href") == "https://example.com/data.tif"
        else:
            feat_assets = getattr(feature, "assets", None)
            assert feat_assets is not None, "assets missing from unprojected feature"
            assert feat_assets.get("data", {}).get("href") == "https://example.com/data.tif"

    def test_non_namespaced_props_survive_round_trip(self):
        """Non-namespaced properties (datetime, title) survive the cycle too."""
        user_props = dict(_FULL_PROPERTIES)
        source = self._build_canonical(user_props)
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        extras = props.pop("extras", {}) or {}
        all_props = {**props, **extras}
        assert "datetime" in all_props, (
            f"datetime missing after round-trip; props={sorted(all_props.keys())}"
        )

    def test_system_keys_do_not_leak_into_properties(self):
        """SYSTEM_FIELD_KEYS must never appear in Feature.properties after round-trip."""
        from dynastore.modules.storage.computed_fields import SYSTEM_FIELD_KEYS

        user_props = dict(_FULL_PROPERTIES)
        source = self._build_canonical(user_props)
        feature = self._unproject(source)
        props = dict(feature.properties or {})
        for sk in SYSTEM_FIELD_KEYS:
            assert sk not in props, (
                f"SYSTEM_FIELD_KEY '{sk}' leaked into Feature.properties: {props}"
            )


# ---------------------------------------------------------------------------
# (d) COLUMNAR mode — _pristine_item path does NOT apply
# ---------------------------------------------------------------------------


class TestColumnarModeUnaffected:
    """The ``_pristine_item`` JSONB short-cut must NOT be triggered in COLUMNAR
    mode.  COLUMNAR builds a column-by-column payload from the schema — it has
    no ``attributes`` JSONB blob at all.  This test verifies that a COLUMNAR
    sidecar with a minimal schema does not emit an ``attributes`` key and does
    not carry colon-namespaced keys (which have no declared column)."""

    def test_columnar_payload_has_no_attributes_blob(self):
        """COLUMNAR sidecar must not emit an ``attributes`` key in the payload."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeSchemaEntry,
        )

        raw_item = _make_raw_item(properties={"datetime": "2024-01-15T00:00:00Z"})
        ctx = _minimal_context(raw_item)
        # Minimal COLUMNAR schema: only a 'datetime' column
        schema_entry = AttributeSchemaEntry(
            name="datetime",
            data_type="string",
            nullable=True,
            unique=False,
        )
        sidecar = FeatureAttributeSidecar(
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.COLUMNAR,
                attribute_schema=[schema_entry],
            )
        )
        payload = sidecar.prepare_upsert_payload(raw_item, ctx)
        assert payload is not None
        assert "attributes" not in payload, (
            f"COLUMNAR sidecar must not emit an 'attributes' blob; "
            f"payload keys={sorted(payload.keys())}"
        )

    def test_columnar_payload_has_no_colon_namespaced_keys(self):
        """COLUMNAR sidecar schema columns must not include colon-namespaced keys
        (they have no declared column → they are silently excluded, not stored)."""
        from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
            AttributeSchemaEntry,
        )

        raw_item = _make_raw_item()
        ctx = _minimal_context(raw_item)
        schema_entry = AttributeSchemaEntry(
            name="datetime",
            data_type="string",
            nullable=True,
            unique=False,
        )
        sidecar = FeatureAttributeSidecar(
            FeatureAttributeSidecarConfig(
                storage_mode=AttributeStorageMode.COLUMNAR,
                attribute_schema=[schema_entry],
            )
        )
        payload = sidecar.prepare_upsert_payload(raw_item, ctx)
        assert payload is not None
        colon_keys = [k for k in payload if ":" in k]
        assert not colon_keys, (
            f"COLUMNAR payload must not contain colon-namespaced keys; "
            f"found: {colon_keys}"
        )
