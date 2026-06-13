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

"""Pin-guard: GeoID's declared ogc-dimensions conformance URIs must match
the set of Building Blocks shipped by the pinned ogc-dimensions commit.

If the upstream spec adds/renames/removes a Building Block, this test
fails until the developer (a) bumps the pin in pyproject.toml and
(b) updates the OGC_DIMENSIONS_URIS list in dimensions_extension.py. This is the
lightweight stand-in for the cross-repo CI URI diff required by the
OGC Dimensions pre-submission fix list (W7).
"""

from dynastore.extensions.dimensions.dimensions_extension import OGC_DIMENSIONS_URIS

EXPECTED_URIS = {
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/core",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-collection",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-member",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-pagination",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-inverse",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-hierarchical",
    "http://www.opengis.net/spec/ogc-dimensions/1.0/conf/dimension-similarity",
}


def test_conformance_uris_match_pinned_building_blocks():
    declared = set(OGC_DIMENSIONS_URIS)
    missing = EXPECTED_URIS - declared
    extra = declared - EXPECTED_URIS
    assert not missing and not extra, (
        f"ogc-dimensions conformance URIs drifted from pinned spec.\n"
        f"  missing (BB shipped upstream but not advertised here): {sorted(missing)}\n"
        f"  extra   (advertised here but no matching BB upstream):  {sorted(extra)}\n"
        f"Bump the git pin in pyproject.toml and sync OGC_DIMENSIONS_URIS."
    )
