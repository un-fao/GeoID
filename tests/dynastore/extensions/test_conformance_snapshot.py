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

EXPECTED_PASS1_STYLES_URIS = {
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/manage-styles",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/style-info",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/mapbox-style",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-10",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/sld-11",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/html",
    "http://www.opengis.net/spec/ogcapi-styles-1/1.0/conf/json",
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


def _read_uri_list_from_source(module_path: str, var_name: str) -> set:
    """Read a URI list constant from a source file without importing.

    Some modules depend on heavy native libraries (``osgeo`` for maps,
    ``lxml`` for styles) that ship from the production base image but
    aren't installed in local dev venvs. Parsing the module's AST avoids
    the heavy import while still exercising the actual source-of-truth.
    """
    import ast
    from pathlib import Path

    src = Path(__file__).resolve().parents[3] / "src" / "dynastore" / module_path
    tree = ast.parse(src.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    if isinstance(node.value, ast.List):
                        return {
                            elt.value for elt in node.value.elts
                            if isinstance(elt, ast.Constant)
                        }
    raise AssertionError(f"{var_name} not found in {module_path}")


def _read_maps_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/maps/maps_service.py", "OGC_API_MAPS_URIS",
    )


def _read_styles_conformance_uris_from_source() -> set:
    return _read_uri_list_from_source(
        "extensions/styles/styles_service.py", "OGC_API_STYLES_URIS",
    )


def test_maps_declares_jpeg_and_geotiff_conformance():
    declared = _read_maps_conformance_uris_from_source()
    missing = EXPECTED_PASS1_MAPS_URIS - declared
    assert not missing, f"Maps extension missing Pass 1 URIs: {sorted(missing)}"
    # PNG must remain declared (Pass 1 adds alongside, never replaces).
    assert "http://www.opengis.net/spec/ogcapi-maps-1/1.0/conf/png" in declared


def test_styles_declares_full_conformance_set():
    declared = _read_styles_conformance_uris_from_source()
    missing = EXPECTED_PASS1_STYLES_URIS - declared
    assert not missing, f"Styles extension missing Pass 1 URIs: {sorted(missing)}"


def test_full_pass1_uri_set_is_declared_somewhere():
    """Belt-and-suspenders: aggregate all Pass 1 URIs and assert none are missing."""
    from dynastore.extensions.records.records_service import OGC_API_RECORDS_URIS
    from dynastore.extensions.web.web import WEB_CONFORMANCE_URIS

    all_declared = (
        set(OGC_API_RECORDS_URIS)
        | set(WEB_CONFORMANCE_URIS)
        | _read_maps_conformance_uris_from_source()
        | _read_styles_conformance_uris_from_source()
    )
    all_expected = (
        EXPECTED_PASS1_RECORDS_URIS
        | EXPECTED_PASS1_WEB_URIS
        | EXPECTED_PASS1_MAPS_URIS
        | EXPECTED_PASS1_STYLES_URIS
    )
    missing = all_expected - all_declared
    assert not missing, (
        f"Pass 1 conformance regression — these URIs are no longer declared: "
        f"{sorted(missing)}"
    )
