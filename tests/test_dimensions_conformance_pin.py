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
