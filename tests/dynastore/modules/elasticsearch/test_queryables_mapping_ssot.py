"""Unit tests for build_known_fields_from_queryables (#1285 PR-1).

Three anchor tests:

* **a** — a FieldDefinition declaring a new queryable field appears in the
  derived mapping with the correct ES type.
* **b** — when no queryable_fields are supplied the no-queryables path
  returns a result byte-identical to the current hand-curated Tier-1 output.
* **c** — SSOT-derived types win over the static Tier-1 guess on overlap.

All tests are pure (no DB fixtures, no network, no async).
"""
from __future__ import annotations

from typing import Dict

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.elasticsearch.items_projection import (
    TIER_1_FIELDS,
    build_known_fields,
    build_known_fields_from_queryables,
)
from dynastore.modules.elasticsearch.mappings import build_item_mapping


# --- helpers -----------------------------------------------------------


def _fd(name: str, data_type: str, container: str = "properties") -> FieldDefinition:
    """Minimal FieldDefinition factory for test use."""
    return FieldDefinition(name=name, data_type=data_type, container=container)  # type: ignore[call-arg]


# --- a -----------------------------------------------------------------


def test_new_queryable_field_appears_in_derived_mapping() -> None:
    """A FieldDefinition added to queryable_fields surfaces in the built mapping.

    The field does not exist in Tier-1 so it cannot reach the mapping via
    the static hand-curated path. build_known_fields_from_queryables must
    carry it through.
    """
    qf: Dict[str, FieldDefinition] = {
        "schema:soil_ph": _fd("schema:soil_ph", "double"),
        "schema:crop_code": _fd("schema:crop_code", "string"),
        "schema:year": _fd("schema:year", "integer"),
        "schema:obs_date": _fd("schema:obs_date", "timestamp"),
        "schema:is_valid": _fd("schema:is_valid", "boolean"),
    }
    known = build_known_fields_from_queryables(qf)
    mapping = build_item_mapping(known)
    props = mapping["properties"]["properties"]["properties"]

    # All five schema fields must appear in properties (container=properties).
    for field_name in qf:
        assert field_name in props, f"{field_name!r} missing from derived mapping"

    # Verify the ES type fragment for each canonical data_type.
    assert props["schema:soil_ph"] == {"type": "double"}
    assert props["schema:crop_code"] == {"type": "keyword"}
    assert props["schema:year"] == {"type": "long"}
    assert props["schema:obs_date"] == {"type": "date"}
    assert props["schema:is_valid"] == {"type": "boolean"}


def test_new_queryable_stats_field_appears_in_stats_container() -> None:
    """A FieldDefinition with container='stats' lands in the stats sub-object."""
    qf: Dict[str, FieldDefinition] = {
        "my_area": _fd("my_area", "double", container="stats"),
    }
    known = build_known_fields_from_queryables(qf)
    mapping = build_item_mapping(known)
    assert "stats" in mapping["properties"]
    stats_props = mapping["properties"]["stats"]["properties"]
    assert "my_area" in stats_props
    assert stats_props["my_area"] == {"type": "double"}


def test_new_queryable_system_field_appears_in_system_container() -> None:
    """A FieldDefinition with container='system' lands in the system sub-object."""
    qf: Dict[str, FieldDefinition] = {
        "custom_hash": _fd("custom_hash", "string", container="system"),
    }
    known = build_known_fields_from_queryables(qf)
    mapping = build_item_mapping(known)
    assert "system" in mapping["properties"]
    sys_props = mapping["properties"]["system"]["properties"]
    assert "custom_hash" in sys_props


# --- b -----------------------------------------------------------------


def test_no_queryables_path_is_byte_identical_to_tier1_only() -> None:
    """build_known_fields_from_queryables({}) equals build_known_fields(None).

    Passing an empty queryable_fields dict should produce exactly the same
    known-fields map as the legacy Tier-1-only call — the backward-compat
    guarantee.
    """
    legacy = build_known_fields(None)
    derived = build_known_fields_from_queryables({})
    assert derived == legacy


def test_no_queryables_mapping_byte_identical_to_item_mapping_tier1() -> None:
    """build_item_mapping of the SSOT path with no fields == ITEM_MAPPING.

    build_item_mapping(build_known_fields_from_queryables({})) must produce
    the same mapping document as the module-level ITEM_MAPPING constant
    (which is build_item_mapping(build_known_fields())).
    """
    from dynastore.modules.elasticsearch.mappings import ITEM_MAPPING

    derived_mapping = build_item_mapping(build_known_fields_from_queryables({}))
    assert derived_mapping == ITEM_MAPPING


def test_tier1_fields_still_present_when_queryables_provided() -> None:
    """Tier-1 fields survive as the fallback even when SSOT fields are present."""
    qf: Dict[str, FieldDefinition] = {
        "schema:new_field": _fd("schema:new_field", "double"),
    }
    known = build_known_fields_from_queryables(qf)

    # A representative sample of Tier-1 fields must still be present.
    for t1_key in ("datetime", "eo:cloud_cover", "proj:epsg", "sar:polarizations"):
        assert t1_key in known, f"Tier-1 field {t1_key!r} dropped from known fields"


# --- c -----------------------------------------------------------------


def test_ssot_derived_type_wins_over_tier1_guess_on_overlap() -> None:
    """When the SSOT declares a field that also exists in Tier-1, SSOT wins.

    Tier-1 has ``proj:epsg`` mapped as ``{"type": "integer"}``. If the
    queryable SSOT declares the same field with a different canonical type,
    the SSOT-derived FieldDefinition object (not the Tier-1 plain dict)
    must win in the known-fields map.
    """
    # Check that Tier-1 does indeed have proj:epsg as a plain dict.
    assert "proj:epsg" in TIER_1_FIELDS
    assert TIER_1_FIELDS["proj:epsg"] == {"type": "integer"}

    # Declare the same field via the SSOT as double instead.
    qf: Dict[str, FieldDefinition] = {
        "proj:epsg": _fd("proj:epsg", "double"),
    }
    known = build_known_fields_from_queryables(qf)

    # The value in known must be the FieldDefinition object (SSOT wins),
    # not the Tier-1 plain dict.
    winner = known["proj:epsg"]
    assert hasattr(winner, "data_type"), (
        "Expected a FieldDefinition (SSOT) in known fields for 'proj:epsg', "
        f"got {type(winner)!r}"
    )
    assert winner.data_type == "double"


def test_ssot_wins_applied_through_build_item_mapping() -> None:
    """The SSOT-wins-on-overlap rule propagates through build_item_mapping.

    When SSOT declares ``proj:epsg`` as ``double``, the mapping fragment for
    that field must be ``{"type": "double"}``, overriding the Tier-1 integer.
    """
    qf: Dict[str, FieldDefinition] = {
        "proj:epsg": _fd("proj:epsg", "double"),
    }
    mapping = build_item_mapping(build_known_fields_from_queryables(qf))
    props = mapping["properties"]["properties"]["properties"]
    assert props["proj:epsg"] == {"type": "double"}, (
        "SSOT-derived type double did not win over Tier-1 integer for proj:epsg"
    )


def test_tier2_overlay_wins_over_ssot() -> None:
    """Tier-2 raw ES dict wins over SSOT FieldDefinition on the same key.

    Tier-2 is the operator's explicit override — it must always take
    precedence over the SSOT-derived type.
    """
    class _FakeCatalogConfig:
        mapping = {"schema:temperature": {"type": "float"}}

    qf: Dict[str, FieldDefinition] = {
        # SSOT says double; operator Tier-2 says float.
        "schema:temperature": _fd("schema:temperature", "double"),
    }
    known = build_known_fields_from_queryables(qf, catalog_config=_FakeCatalogConfig())
    # Tier-2 raw dict wins.
    assert known["schema:temperature"] == {"type": "float"}


def test_build_known_fields_from_queryables_is_pure_no_side_effects() -> None:
    """Calling the function must not mutate TIER_1_FIELDS."""
    original_tier1 = dict(TIER_1_FIELDS)
    qf: Dict[str, FieldDefinition] = {
        "schema:extra": _fd("schema:extra", "string"),
    }
    build_known_fields_from_queryables(qf)
    assert dict(TIER_1_FIELDS) == original_tier1, "TIER_1_FIELDS was mutated"
