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

"""``_collect_generated_stats`` — flatten one item's computed statistics.

This is the ingestion-side companion to the read-path expose loop: it surfaces
the *full* computed set (regardless of ``ItemsReadPolicy.feature_type.expose``)
from the in-memory sidecar payloads the upsert already holds. Pure function, so
these run with no DB.
"""

import json

from dynastore.modules.catalog.item_service import _collect_generated_stats


def test_geom_stats_dict_blob_is_flattened() -> None:
    """The geometries sidecar leaves ``geom_stats`` a plain dict."""
    payloads = {
        "geometries": {
            "geoid": "g1",
            "geom_type": "Polygon",
            "geom_stats": {"area": 7845.0, "perimeter": 412.3},
        }
    }
    out = _collect_generated_stats(payloads, stat_names=set())
    assert out == {"area": 7845.0, "perimeter": 412.3}


def test_attribute_stats_json_string_blob_is_parsed() -> None:
    """The attributes sidecar ``json.dumps`` its ``attribute_stats`` blob."""
    payloads = {
        "feature_attributes": {
            "geoid": "g1",
            "attribute_stats": json.dumps({"population_density": 123.4}),
        }
    }
    out = _collect_generated_stats(payloads, stat_names=set())
    assert out == {"population_density": 123.4}


def test_columnar_stats_picked_by_name_only() -> None:
    """COLUMNAR stats live at the payload top level mixed with non-stat keys;
    only the resolved-names in ``stat_names`` are collected."""
    payloads = {
        "geometries": {
            "geoid": "g1",
            "geom": "0103...wkb",          # geometry — never a stat
            "geom_type": "Polygon",         # structural — never a stat
            "h3_res7": "87283472bffffff",   # spatial index — never a stat
            "area": 7845.0,                 # COLUMNAR stat
            "vertex_count": 5,              # COLUMNAR stat
        }
    }
    out = _collect_generated_stats(payloads, stat_names={"area", "vertex_count"})
    assert out == {"area": 7845.0, "vertex_count": 5}
    assert "geom" not in out
    assert "geom_type" not in out
    assert "h3_res7" not in out


def test_blobs_and_columnar_merge_across_sidecars() -> None:
    out = _collect_generated_stats(
        {
            "geometries": {
                "geoid": "g1",
                "geom_stats": {"area": 10.0},
                "centroid": "0101...wkb",  # COLUMNAR centroid (stored WKB)
            },
            "feature_attributes": {
                "geoid": "g1",
                "attribute_stats": json.dumps({"mean_value": 3.5}),
            },
        },
        stat_names={"centroid"},
    )
    assert out == {"area": 10.0, "centroid": "0101...wkb", "mean_value": 3.5}


def test_no_stats_yields_empty_dict() -> None:
    out = _collect_generated_stats(
        {"geometries": {"geoid": "g1", "geom_type": "Point"}}, stat_names=set()
    )
    assert out == {}


def test_malformed_json_blob_is_skipped_not_raised() -> None:
    out = _collect_generated_stats(
        {"feature_attributes": {"geoid": "g1", "attribute_stats": "{not json"}},
        stat_names=set(),
    )
    assert out == {}
