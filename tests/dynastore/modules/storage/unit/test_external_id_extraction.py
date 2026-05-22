"""Regression coverage for external_id extraction from GDAL/shapefile features.

Issue #488: when ``items_write_policy.external_id_field`` named a non-``id``
attribute (e.g. ``ADM2_PCODE``) the extracted ``external_id`` was empty in DB,
because ``_extract_value`` only fell back to ``properties`` when the path
literally contained ``id`` or ``asset_id``. GDAL features arrive as
``{"properties": {...}, "geometry": ...}`` so the lookup at the root layer
missed every user-defined field.

The ``external_id_field`` knob was folded into
:attr:`ItemsWritePolicy.derive` (a ComputedField of kind EXTERNAL_ID whose
``name`` is the source path). The sidecar still receives the policy on
``context["_items_write_policy"]``; this module covers the still-live
``_extract_value`` resolution. The separate require-external-id guard layer
was removed in #1164 (it never fired in production — the wire schema's
``required`` list was never populated on the policy); a missing/empty
external_id that is a declared required field is now rejected by the normal
required-field check plus NOT NULL columns.
"""
from __future__ import annotations

from dynastore.modules.storage.drivers.pg_sidecars.attributes import (
    FeatureAttributeSidecar,
)
from dynastore.modules.storage.drivers.pg_sidecars.attributes_config import (
    FeatureAttributeSidecarConfig,
)


def _sidecar() -> FeatureAttributeSidecar:
    return FeatureAttributeSidecar(FeatureAttributeSidecarConfig())


class TestExtractValuePropertiesFallback:
    def test_root_level_wins(self):
        sidecar = _sidecar()
        feature = {"code": "ROOT", "properties": {"code": "PROPS"}}
        assert sidecar._extract_value(feature, "code") == "ROOT"

    def test_falls_back_to_properties_for_arbitrary_field(self):
        sidecar = _sidecar()
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"ADM2_PCODE": "TG0309"},
        }
        assert sidecar._extract_value(feature, "ADM2_PCODE") == "TG0309"

    def test_legacy_id_path_still_works(self):
        sidecar = _sidecar()
        feature = {"properties": {"id": "X"}}
        assert sidecar._extract_value(feature, "id") == "X"

    def test_asset_id_path_still_works(self):
        sidecar = _sidecar()
        feature = {"properties": {"asset_id": "A"}}
        assert sidecar._extract_value(feature, "asset_id") == "A"

    def test_dot_path_does_not_trigger_fallback(self):
        sidecar = _sidecar()
        feature = {"properties": {"ADM2_PCODE": "TG0309"}}
        assert (
            sidecar._extract_value(feature, "properties.ADM2_PCODE") == "TG0309"
        )

    def test_missing_field_returns_none(self):
        sidecar = _sidecar()
        feature = {"properties": {"OTHER": "x"}}
        assert sidecar._extract_value(feature, "ADM2_PCODE") is None

    def test_no_properties_bag_returns_none(self):
        sidecar = _sidecar()
        assert sidecar._extract_value({"geometry": None}, "ADM2_PCODE") is None
