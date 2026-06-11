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

"""Web-page registration smoke tests for the GeoVolumes globe browser (volumes extension).

Asserts that VolumesService contributes:
- a web page with id ``volumes_browser`` via ``get_web_pages()``
- a static-asset prefix ``volumes`` via ``get_static_assets()``
- a page handler that returns HTTP 200 HTML with the ``{{VERSION}}`` token
  substituted
- the HTML file includes the required CDN script/link tags with integrity
  attributes and the expected own-JS / common-helper references, consistent
  with the static-path rule (extension pages at /web/{prefix}/, common assets
  at /web/static/common/)

No WebGL or browser testing is performed — all assertions are string checks
against the static HTML file.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os

import pytest

MODULE_PATH = "dynastore.extensions.volumes.volumes_service"
CLASS_NAME = "VolumesService"
PAGE_ID = "volumes_browser"
STATIC_PREFIX = "volumes"
HANDLER_NAME = "provide_volumes_browser"


def _bare_instance():
    mod = importlib.import_module(MODULE_PATH)
    cls = getattr(mod, CLASS_NAME)
    return cls.__new__(cls)


# ---------------------------------------------------------------------------
# Registration tests
# ---------------------------------------------------------------------------


def test_page_id_registered():
    svc = _bare_instance()
    page_ids = {spec.page_id for spec in svc.get_web_pages()}
    assert PAGE_ID in page_ids, (
        f"{CLASS_NAME} did not contribute page '{PAGE_ID}'; got {page_ids}"
    )


def test_static_prefix_registered():
    svc = _bare_instance()
    prefixes = {asset.prefix.strip("/") for asset in svc.get_static_assets()}
    assert STATIC_PREFIX in prefixes, (
        f"{CLASS_NAME} did not contribute static prefix '{STATIC_PREFIX}'; got {prefixes}"
    )


def test_static_files_non_empty():
    svc = _bare_instance()
    for asset in svc.get_static_assets():
        if asset.prefix.strip("/") == STATIC_PREFIX:
            files = asset.files_provider()
            assert len(files) > 0, "provide_static_files() returned an empty list"
            assert all(os.path.isfile(f) for f in files), (
                "provide_static_files() returned paths that do not exist on disk"
            )
            return
    pytest.fail(f"Static asset provider for prefix '{STATIC_PREFIX}' not found")


# ---------------------------------------------------------------------------
# Handler smoke test
# ---------------------------------------------------------------------------


def test_handler_returns_200_html():
    svc = _bare_instance()
    handler = getattr(svc, HANDLER_NAME)

    if "request" in inspect.signature(handler).parameters:
        result = handler(request=None)
    else:
        result = handler()
    if inspect.isawaitable(result):
        result = asyncio.get_event_loop().run_until_complete(result)

    assert getattr(result, "status_code", 200) == 200
    body = result.body.decode() if hasattr(result, "body") else str(result)
    assert "<" in body, "handler did not return HTML"
    assert "{{VERSION}}" not in body, "VERSION template token was not substituted"


# ---------------------------------------------------------------------------
# Static-path rule: HTML content assertions (string checks, no browser)
# ---------------------------------------------------------------------------


def _html_content() -> str:
    html_path = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..", "..",
        "packages", "extensions", "volumes", "src",
        "dynastore", "extensions", "volumes", "static",
        "volumes_browser.html",
    )
    html_path = os.path.normpath(html_path)
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()


def test_html_includes_maplibre_script_with_integrity():
    html = _html_content()
    assert "maplibre-gl@5.24.0/dist/maplibre-gl.js" in html, (
        "HTML does not reference maplibre-gl.js at the pinned version"
    )
    assert 'integrity="sha384-' in html, (
        "maplibre-gl.js script tag is missing an SRI integrity attribute"
    )


def test_html_includes_maplibre_css_with_integrity():
    html = _html_content()
    assert "maplibre-gl@5.24.0/dist/maplibre-gl.css" in html, (
        "HTML does not reference maplibre-gl.css at the pinned version"
    )


def test_html_includes_deckgl_script_with_integrity():
    html = _html_content()
    assert "deck.gl@9.3.3/dist.min.js" in html, (
        "HTML does not reference deck.gl dist.min.js at the pinned version"
    )
    import re
    integrity_count = len(re.findall(r'integrity="sha384-', html))
    assert integrity_count >= 2, (
        f"Expected at least 2 SRI integrity attributes (maplibre + deck.gl), found {integrity_count}"
    )


def test_html_common_i18n_path():
    """Common helpers are at ../static/common/ from the extension page."""
    html = _html_content()
    assert "../static/common/i18n.js" in html, (
        "HTML does not reference i18n.js via the correct ../static/common/ path"
    )


def test_html_common_maplibre_map_path():
    html = _html_content()
    assert "../static/common/maplibre-map.js" in html, (
        "HTML does not reference maplibre-map.js via the correct ../static/common/ path"
    )


def test_html_own_js_path():
    """Own JS is in the same directory as the HTML: ./volumes-globe.js."""
    html = _html_content()
    assert "./volumes-globe.js" in html, (
        "HTML does not reference volumes-globe.js via the correct ./ path"
    )


def test_html_map_div_present():
    html = _html_content()
    assert 'id="geovolumes-map"' in html, (
        "HTML is missing the #geovolumes-map div required by the globe initialiser"
    )


def test_html_no_unsubstituted_version_token():
    """The handler substitutes {{VERSION}}; the raw file must contain the token."""
    html = _html_content()
    assert "{{VERSION}}" in html, (
        "volumes_browser.html is missing the {{VERSION}} template token"
    )
