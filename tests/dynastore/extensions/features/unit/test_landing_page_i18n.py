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

"""Landing-page i18n integration: features service via TestClient.

The features landing page does NOT route through ``ogc_landing_page_handler``;
it builds a ``LandingPage`` via ``ogc_generator.create_landing_page`` and must
localize it explicitly with ``localize_model`` before returning. Without that,
``model_dump()`` emits the language-keyed ``{"en": ...}`` dicts produced by the
``Link``/``LandingPage`` title validators instead of resolved strings. These
tests guard that regression: both the top-level ``title``/``description`` and
each ``links[].title`` must collapse to a single language by default, and
``lang=*`` must preserve the full multi-language dict.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.features.features_service import OGCFeaturesService


def _build_client() -> TestClient:
    """Return a TestClient bound to a features service with a fresh FastAPI app."""
    app = FastAPI()
    svc = OGCFeaturesService(app=app)
    app.include_router(svc.router)
    return TestClient(app)


def test_landing_page_no_lang_title_and_links_are_plain_strings():
    """No lang parameter → top-level title/description and link titles are plain str."""
    client = _build_client()
    r = client.get("/features/")
    assert r.status_code == 200, r.text
    body = r.json()
    assert isinstance(body.get("title"), str), (
        f"Expected plain str landing title, got {body.get('title')!r}"
    )
    if body.get("description") is not None:
        assert isinstance(body["description"], str)
    assert "links" in body
    for link in body["links"]:
        if "title" in link:
            assert isinstance(link["title"], str), (
                f"Expected plain str for link title, got {link['title']!r}"
            )


def test_landing_page_lang_fr_falls_back_to_en_on_en_only_titles():
    """?lang=fr on English-only titles → plain str (en fallback), never a dict."""
    client = _build_client()
    r = client.get("/features/", params={"lang": "fr"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert isinstance(body.get("title"), str)
    for link in body["links"]:
        if "title" in link:
            assert isinstance(link["title"], str)


def test_landing_page_lang_wildcard_preserves_link_title_dicts():
    """?lang=* → the full multi-language dict is preserved on each link title.

    ``LandingPage.title``/``description`` are plain ``str`` fixed labels (not
    ``LocalizedText``), so they stay plain strings; the localizable carriers
    here are the ``links[].title`` values, which must keep their dict shape.
    """
    client = _build_client()
    r = client.get("/features/", params={"lang": "*"})
    assert r.status_code == 200, r.text
    body = r.json()
    # Fixed-label top-level title is never a multi-language object.
    assert isinstance(body.get("title"), str)
    for link in body["links"]:
        if "title" in link:
            assert isinstance(link["title"], dict), (
                f"Expected dict for lang=*, got {link['title']!r}"
            )
            assert "en" in link["title"]


def test_landing_page_lang_query_conflicts_with_accept_language_returns_400():
    """?lang=en + Accept-Language: fr → 400 conflict enforced by get_language."""
    client = _build_client()
    r = client.get(
        "/features/",
        params={"lang": "en"},
        headers={"Accept-Language": "fr"},
    )
    assert r.status_code == 400, r.text
