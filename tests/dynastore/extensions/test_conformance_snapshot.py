"""
Pass 1 conformance snapshot test.

Pins the set of OGC API conformance URIs Pass 1 declares, so a careless
refactor that drops a URI trips the test loudly. The test reads each
extension's conformance-URI constants directly — no server boot required.

The full ``/conformance`` HTTP endpoint is covered by existing
integration tests; this snapshot is the unit-level guard.
"""

from __future__ import annotations


EXPECTED_PASS1_RECORDS_URIS = {
    "http://www.opengis.net/spec/ogcapi-records-1/1.0/conf/geojson",  # T5
}

EXPECTED_PASS1_WEB_URIS = {
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas31",  # T6
}

EXPECTED_PASS1_MAPS_URIS = {
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/jpeg",       # T7
    "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/geotiff",    # T7
}


def test_records_declares_geojson_conformance():
    from dynastore.extensions.records.records_service import OGC_API_RECORDS_URIS
    declared = set(OGC_API_RECORDS_URIS)
    missing = EXPECTED_PASS1_RECORDS_URIS - declared
    assert not missing, f"Records extension missing Pass 1 URIs: {sorted(missing)}"


def test_web_declares_oas31_conformance():
    from dynastore.extensions.web.web import WEB_CONFORMANCE_URIS
    declared = set(WEB_CONFORMANCE_URIS)
    missing = EXPECTED_PASS1_WEB_URIS - declared
    assert not missing, f"Web extension missing Pass 1 URIs: {sorted(missing)}"
    # OAS 3.0 must remain declared (Pass 1 adds 3.1 alongside, not in place of).
    assert "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30" in declared


def _read_maps_conformance_uris_from_source() -> set:
    """Read OGC_API_MAPS_URIS from the source file without importing the module.

    ``maps_service`` imports ``osgeo`` at module scope via ``renderer``;
    that dependency ships from the GDAL base image in production but isn't
    installed in local dev venvs. Parsing the module's AST avoids the
    heavy import while still exercising the actual source-of-truth.
    """
    import ast
    from pathlib import Path

    src = Path(__file__).resolve().parents[3] / "src" / "dynastore" / "extensions" / "maps" / "maps_service.py"
    tree = ast.parse(src.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "OGC_API_MAPS_URIS":
                    if isinstance(node.value, ast.List):
                        return {elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)}
    raise AssertionError("OGC_API_MAPS_URIS not found in maps_service.py")


def test_maps_declares_jpeg_and_geotiff_conformance():
    declared = _read_maps_conformance_uris_from_source()
    missing = EXPECTED_PASS1_MAPS_URIS - declared
    assert not missing, f"Maps extension missing Pass 1 URIs: {sorted(missing)}"
    # PNG must remain declared (Pass 1 adds alongside, never replaces).
    assert "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png" in declared


def test_full_pass1_uri_set_is_declared_somewhere():
    """Belt-and-suspenders: aggregate all Pass 1 URIs and assert none are missing."""
    from dynastore.extensions.records.records_service import OGC_API_RECORDS_URIS
    from dynastore.extensions.web.web import WEB_CONFORMANCE_URIS

    all_declared = (
        set(OGC_API_RECORDS_URIS)
        | set(WEB_CONFORMANCE_URIS)
        | _read_maps_conformance_uris_from_source()
    )
    all_expected = (
        EXPECTED_PASS1_RECORDS_URIS
        | EXPECTED_PASS1_WEB_URIS
        | EXPECTED_PASS1_MAPS_URIS
    )
    missing = all_expected - all_declared
    assert not missing, (
        f"Pass 1 conformance regression — these URIs are no longer declared: "
        f"{sorted(missing)}"
    )
