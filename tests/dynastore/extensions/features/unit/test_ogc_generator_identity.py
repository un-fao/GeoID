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

# Copyright 2025 FAO
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""#1232 — the create/update echo (and raw-row read fallback) must not surface
identity inside ``properties``. ``_db_row_to_ogc_feature`` is the single
boundary where both Postgres and Elasticsearch responses assemble their
``properties`` from stored data, so a structural member that rode in (the PG
``feature_id_expr AS id`` alias, or the ES write echo returning the in-memory
feature as-is) must be stripped here to match the GET read contract.
"""
from __future__ import annotations

from geojson_pydantic import Feature as GeoJSONFeature

from dynastore.extensions.features.ogc_generator import _db_row_to_ogc_feature


def _make_feature(properties: dict) -> GeoJSONFeature:
    return GeoJSONFeature(
        type="Feature",
        id="019e507b-64c9-72a1-8d75-26c015777147",
        geometry=None,
        properties=properties,
    )


def test_echo_strips_leaked_id_from_properties() -> None:
    item = _make_feature(
        {
            "datetime": "2024-01-15T00:00:00Z",
            "id": "019e507b-64c9-72a1-8d75-26c015777147",
        }
    )
    out = _db_row_to_ogc_feature(
        item,
        catalog_id="cat",
        collection_id="col",
        root_url="http://localhost",
    )
    assert out.properties == {"datetime": "2024-01-15T00:00:00Z"}
    # Canonical identity remains at the feature top level.
    assert str(out.id) == "019e507b-64c9-72a1-8d75-26c015777147"


def test_echo_preserves_geoid_property_when_exposed() -> None:
    # expose_geoid surfaces a ``geoid`` property (not a reserved member); only
    # structural identity (``id``) is dropped.
    item = _make_feature(
        {
            "datetime": "2024-01-15T00:00:00Z",
            "id": "019e507b-64c9-72a1-8d75-26c015777147",
            "geoid": "019e507b-64c9-72a1-8d75-26c015777147",
        }
    )
    out = _db_row_to_ogc_feature(
        item,
        catalog_id="cat",
        collection_id="col",
        root_url="http://localhost",
    )
    assert out.properties == {
        "datetime": "2024-01-15T00:00:00Z",
        "geoid": "019e507b-64c9-72a1-8d75-26c015777147",
    }


def test_echo_drops_all_structural_members_from_properties() -> None:
    item = _make_feature(
        {
            "datetime": "2024-01-15T00:00:00Z",
            "id": "feat",
            "type": "Feature",
            "collection": "col",
            "links": [],
            "assets": {},
            "stac_version": "1.0.0",
            "stac_extensions": [],
        }
    )
    out = _db_row_to_ogc_feature(
        item,
        catalog_id="cat",
        collection_id="col",
        root_url="http://localhost",
    )
    assert out.properties == {"datetime": "2024-01-15T00:00:00Z"}
