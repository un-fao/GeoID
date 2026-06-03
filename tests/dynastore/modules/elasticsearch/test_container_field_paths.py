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
"""Task 3 — Field-path resolvers honour the container tag (refs #1800).

Tests that ``resolve_es_field_path``, ``build_es_field_mapping``, and
``parse_sort`` all route fields through the container classifier so:

* stats fields (area, centroid, s2_*, h3_*, geohash_*) → ``stats.<name>``
* system fields (geometry_hash, validity, transaction_time, …) → ``system.<name>``
* identity fields (external_id, asset_id, geoid) → flat ``<name>`` at root
* user / STAC attrs (eo:cloud_cover, datetime, …) → ``properties.<name>`` (UNCHANGED)
* unknown → ``properties.extras.<name>`` (UNCHANGED)

The regression test for #1786 pins that the ``properties.*`` sort path for user
attributes stays UNCHANGED after Task 3.
"""
from __future__ import annotations

import pytest

from dynastore.models.protocols.field_definition import FieldDefinition
from dynastore.modules.elasticsearch.items_projection import (
    build_known_fields,
    resolve_es_field_path,
)
from dynastore.modules.storage.drivers.es_common.cql_to_es import build_es_field_mapping
from dynastore.modules.elasticsearch.items_query import parse_sort


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fd(name: str, data_type: str = "string", container: str = "properties", expose: bool = True) -> FieldDefinition:
    return FieldDefinition(name=name, data_type=data_type, container=container, expose=expose)


def _known_with_containers() -> dict:
    """A known-fields map containing tagged FieldDefinition entries for all containers."""
    return {
        # identity (flat at root)
        "external_id": _fd("external_id", container="identity"),
        "asset_id": _fd("asset_id", container="identity"),
        "geoid": _fd("geoid", container="identity"),
        # system
        "geometry_hash": _fd("geometry_hash", container="system"),
        "attributes_hash": _fd("attributes_hash", container="system"),
        "validity": _fd("validity", container="system"),
        "transaction_time": _fd("transaction_time", data_type="timestamp", container="system"),
        "deleted_at": _fd("deleted_at", data_type="timestamp", container="system"),
        # stats
        "area": _fd("area", data_type="double", container="stats"),
        "centroid": _fd("centroid", container="stats"),
        "s2_7": _fd("s2_7", container="stats"),
        "h3_5": _fd("h3_5", container="stats"),
        "geohash_6": _fd("geohash_6", container="stats"),
        # user / STAC properties (default container)
        "datetime": _fd("datetime", data_type="timestamp"),
        "eo:cloud_cover": _fd("eo:cloud_cover", data_type="double"),
        "platform": _fd("platform"),
    }


# ---------------------------------------------------------------------------
# resolve_es_field_path — stats/system/identity/properties/extras
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "stac_path,expected",
    [
        # Stats → stats.<name>
        ("properties.area", "stats.area"),
        ("properties.centroid", "stats.centroid"),
        ("properties.s2_7", "stats.s2_7"),
        ("properties.h3_5", "stats.h3_5"),
        ("properties.geohash_6", "stats.geohash_6"),
        # System → system.<name>
        ("properties.geometry_hash", "system.geometry_hash"),
        ("properties.attributes_hash", "system.attributes_hash"),
        ("properties.validity", "system.validity"),
        ("properties.transaction_time", "system.transaction_time"),
        ("properties.deleted_at", "system.deleted_at"),
        # Identity → flat root (no container prefix)
        ("external_id", "external_id"),
        ("asset_id", "asset_id"),
        ("geoid", "geoid"),
        # User / STAC properties → properties.<name> (UNCHANGED — #1786 regression pin)
        ("properties.datetime", "properties.datetime"),
        ("properties.eo:cloud_cover", "properties.eo:cloud_cover"),
        ("properties.platform", "properties.platform"),
        # Unknown → properties.extras.<name> (UNCHANGED)
        ("properties.vendor:custom", "properties.extras.vendor:custom"),
        ("properties.fao:score", "properties.extras.fao:score"),
        # Non-properties path passes through unchanged
        ("_external_id", "_external_id"),
        ("id", "id"),
    ],
)
def test_resolve_es_field_path_container_routing(stac_path: str, expected: str) -> None:
    """resolve_es_field_path must route through the container classifier."""
    known = _known_with_containers()
    assert resolve_es_field_path(stac_path, known) == expected


def test_resolve_es_field_path_properties_unchanged_regression_1786() -> None:
    """Regression guard for #1786: properties.* user-attribute sort paths must
    stay at ``properties.<name>`` after Task 3 wiring. This pins zero change
    for the user-attribute sort contract so #1786 fixes are not inadvertently
    reverted."""
    known = _known_with_containers()
    for name in ("datetime", "eo:cloud_cover", "platform"):
        path = f"properties.{name}"
        resolved = resolve_es_field_path(path, known)
        assert resolved == path, (
            f"#1786 regression: {path!r} resolved to {resolved!r}; "
            "expected no change for user-attribute paths"
        )


# ---------------------------------------------------------------------------
# build_es_field_mapping — public doc (private=False)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "field_name,container,expected_path",
    [
        # Stats → stats.<name> (no .keyword suffix — already typed as keyword in mapping)
        ("area", "stats", "stats.area"),
        ("centroid", "stats", "stats.centroid"),
        ("s2_7", "stats", "stats.s2_7"),
        # System → system.<name>
        ("geometry_hash", "system", "system.geometry_hash"),
        ("validity", "system", "system.validity"),
        ("transaction_time", "system", "system.transaction_time"),
        # Identity → flat root
        ("external_id", "identity", "external_id"),
        ("asset_id", "identity", "asset_id"),
        ("geoid", "identity", "geoid"),
    ],
)
def test_build_es_field_mapping_public_container_routing(
    field_name: str, container: str, expected_path: str
) -> None:
    """build_es_field_mapping (public=False) must route fields to their container paths."""
    fields = {
        field_name: _fd(
            field_name,
            data_type="double" if field_name == "area" else "string",
            container=container,
        )
    }
    m = build_es_field_mapping(fields, private=False)
    assert m[field_name] == expected_path, (
        f"{field_name!r} (container={container!r}): expected {expected_path!r}, got {m[field_name]!r}"
    )


def test_build_es_field_mapping_user_attrs_unchanged() -> None:
    """User / STAC attrs must still map to ``properties.<name>`` — no regression."""
    fields = {
        "eo:cloud_cover": _fd("eo:cloud_cover", data_type="double"),
        "datetime": _fd("datetime", data_type="timestamp"),
    }
    m = build_es_field_mapping(fields, private=False)
    # numeric types get no .keyword suffix
    assert m["eo:cloud_cover"] == "properties.eo:cloud_cover"
    assert m["datetime"] == "properties.datetime"


def test_build_es_field_mapping_user_string_attr_keyword_suffix() -> None:
    """String user attributes must still carry the .keyword suffix (regression)."""
    fields = {"platform": _fd("platform", data_type="string")}
    m = build_es_field_mapping(fields, private=False)
    assert m["platform"] == "properties.platform.keyword"


# ---------------------------------------------------------------------------
# build_es_field_mapping — private doc (private=True)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "field_name,container,expected_path",
    [
        # Stats → stats.<name> regardless of private/public
        ("area", "stats", "stats.area"),
        ("centroid", "stats", "stats.centroid"),
        # System → system.<name>
        ("geometry_hash", "system", "system.geometry_hash"),
        # Identity → flat root (same as public)
        ("external_id", "identity", "external_id"),
    ],
)
def test_build_es_field_mapping_private_container_routing(
    field_name: str, container: str, expected_path: str
) -> None:
    """build_es_field_mapping (private=True) must also route by container."""
    fields = {
        field_name: _fd(
            field_name,
            data_type="double" if field_name == "area" else "string",
            container=container,
        )
    }
    m = build_es_field_mapping(fields, private=True)
    assert m[field_name] == expected_path


# ---------------------------------------------------------------------------
# parse_sort — sort path routing
# ---------------------------------------------------------------------------

def test_parse_sort_stats_field() -> None:
    """Sorting by a stats field must target ``stats.<name>``."""
    known = _known_with_containers()
    clause = parse_sort("+properties.area", known)
    # The sort clause must contain the stats path, not properties.area or extras.
    assert clause[0] == {"stats.area": {"order": "asc"}}


def test_parse_sort_system_field() -> None:
    """Sorting by a system field must target ``system.<name>``."""
    known = _known_with_containers()
    clause = parse_sort("-properties.transaction_time", known)
    assert clause[0] == {"system.transaction_time": {"order": "desc"}}


def test_parse_sort_user_attr_unchanged_regression_1786() -> None:
    """Regression guard for #1786: sorting by a user attribute must still
    target ``properties.<name>`` — zero change after Task 3 wiring."""
    known = _known_with_containers()
    # eo:cloud_cover is a user attr (container default = "properties")
    clause = parse_sort("+properties.eo:cloud_cover", known)
    assert clause[0] == {"properties.eo:cloud_cover": {"order": "asc"}}, (
        f"#1786 regression: expected 'properties.eo:cloud_cover', got {clause[0]}"
    )


def test_parse_sort_unknown_routes_to_extras() -> None:
    """Sorting by an unknown field must still route to properties.extras.<name>."""
    known = _known_with_containers()
    clause = parse_sort("+properties.vendor:custom_score", known)
    assert clause[0] == {"properties.extras.vendor:custom_score": {"order": "asc"}}


def test_parse_sort_identity_field_flat() -> None:
    """Sorting by an identity field must target the flat root path."""
    known = _known_with_containers()
    clause = parse_sort("+external_id", known)
    # external_id is not under properties, so it passes through resolve_es_field_path
    # unchanged as a top-level path.
    assert clause[0] == {"external_id": {"order": "asc"}}
