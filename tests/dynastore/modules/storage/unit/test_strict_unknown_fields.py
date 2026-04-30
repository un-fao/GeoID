"""Tests for ``CollectionSchema.strict_unknown_fields`` enforcement.

When set, writes whose features carry properties NOT declared in
``CollectionSchema.fields`` are refused at the service-layer pre-write
helper. System fields (id, geoid, geometry, bbox, properties, etc.)
always pass regardless of declared fields.
"""

from __future__ import annotations

import pytest

from dynastore.modules.storage.errors import UnknownFieldsError
from dynastore.modules.storage.field_constraints import (
    check_strict_unknown_fields,
)


# ---------------------------------------------------------------------------
# Allowed-fields check — properties bag
# ---------------------------------------------------------------------------

def test_passes_when_all_props_declared() -> None:
    features = [{"properties": {"road_id": "r1", "lanes": 2}}]
    check_strict_unknown_fields(["road_id", "lanes"], features)


def test_raises_on_unknown_property() -> None:
    features = [{"properties": {"road_id": "r1", "ROGUE": "x"}}]
    with pytest.raises(UnknownFieldsError) as exc:
        check_strict_unknown_fields(["road_id"], features)
    assert "ROGUE" in exc.value.unknown_fields
    assert "road_id" in exc.value.allowed_fields


def test_first_offending_feature_fails_fast() -> None:
    features = [
        {"properties": {"road_id": "r1"}},
        {"properties": {"road_id": "r2", "extra": "boom"}},
        {"properties": {"road_id": "r3"}},
    ]
    with pytest.raises(UnknownFieldsError) as exc:
        check_strict_unknown_fields(["road_id"], features)
    assert exc.value.unknown_fields == ["extra"]


def test_multiple_unknown_fields_in_message() -> None:
    features = [{"properties": {"a": 1, "b": 2, "c": 3}}]
    with pytest.raises(UnknownFieldsError) as exc:
        check_strict_unknown_fields(["a"], features)
    assert sorted(exc.value.unknown_fields) == ["b", "c"]


# ---------------------------------------------------------------------------
# System fields — always allowed
# ---------------------------------------------------------------------------

def test_system_fields_always_pass() -> None:
    """id, geoid, geometry, bbox, properties etc. are never rejected."""
    features = [{
        "id": "f1",
        "geoid": "abc",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "bbox": [0, 0, 1, 1],
        "type": "Feature",
        "properties": {"road_id": "r1"},
    }]
    # No declared user fields except road_id; system fields all pass
    check_strict_unknown_fields(["road_id"], features)


def test_top_level_unknown_field_rejected() -> None:
    """Non-system top-level keys are also caught."""
    features = [{
        "id": "f1",
        "geometry": None,
        "properties": {"road_id": "r1"},
        "rogue_top_level": "boom",
    }]
    with pytest.raises(UnknownFieldsError) as exc:
        check_strict_unknown_fields(["road_id"], features)
    assert "rogue_top_level" in exc.value.unknown_fields


# ---------------------------------------------------------------------------
# Empty / edge cases
# ---------------------------------------------------------------------------

def test_empty_allowed_fields_rejects_any_user_field() -> None:
    features = [{"properties": {"any_field": 1}}]
    with pytest.raises(UnknownFieldsError):
        check_strict_unknown_fields([], features)


def test_empty_features_no_op() -> None:
    """No features → no check → no raise."""
    check_strict_unknown_fields(["road_id"], [])


def test_feature_with_no_properties_passes() -> None:
    """Geometry-only features (no properties bag) shouldn't trip the check."""
    features = [{"id": "f1", "geometry": {"type": "Point", "coordinates": [0, 0]}}]
    check_strict_unknown_fields(["road_id"], features)


def test_non_mapping_properties_ignored() -> None:
    """If properties is not a Mapping (corrupt input), check skips it gracefully."""
    features = [{"id": "f1", "properties": "not a mapping"}]
    # No raise — non-Mapping properties shape is left for the schema
    # validator downstream; strict-fields helper is intentionally narrow.
    check_strict_unknown_fields(["road_id"], features)


# ---------------------------------------------------------------------------
# UnknownFieldsError shape — for the RFC 9457 mapping
# ---------------------------------------------------------------------------

def test_unknown_fields_error_carries_structured_payload() -> None:
    err = UnknownFieldsError(
        "boom", unknown_fields=["a", "b"], allowed_fields=["x"],
    )
    assert err.unknown_fields == ["a", "b"]
    assert err.allowed_fields == ["x"]
    assert "boom" in str(err)
