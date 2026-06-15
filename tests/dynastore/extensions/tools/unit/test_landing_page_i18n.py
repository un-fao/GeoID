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

"""Landing-page i18n integration: records service via TestClient.

Verifies the four language negotiation scenarios for the OGC landing page:

1. No ``lang`` param → link titles are plain strings (default 'en').
2. ``?lang=fr`` → French title where present, else 'en' fallback.
3. ``?lang=*`` → full multi-language dict preserved on each link title.
4. ``?lang=en`` + ``Accept-Language: fr`` → 400 (conflict enforced by
   ``get_language`` at language_utils.py).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dynastore.extensions.records.records_service import RecordsService


# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------


def _build_client() -> TestClient:
    """Return a TestClient bound to a records service with a fresh FastAPI app."""
    app = FastAPI()
    svc = RecordsService(app=app)
    app.include_router(svc.router)
    return TestClient(app)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_landing_page_no_lang_titles_are_plain_strings():
    """No lang parameter → link titles are plain strings (default 'en')."""
    client = _build_client()
    r = client.get("/records/")
    assert r.status_code == 200, r.text
    body = r.json()
    assert "links" in body
    for link in body["links"]:
        if "title" in link:
            assert isinstance(link["title"], str), (
                f"Expected plain str for link title, got {link['title']!r}"
            )


def test_landing_page_lang_en_titles_are_plain_strings():
    """Explicit ?lang=en → same as default: plain string titles."""
    client = _build_client()
    r = client.get("/records/", params={"lang": "en"})
    assert r.status_code == 200, r.text
    for link in r.json()["links"]:
        if "title" in link:
            assert isinstance(link["title"], str)


def test_landing_page_lang_fr_falls_back_to_en_on_en_only_titles():
    """?lang=fr on titles that only have 'en' → falls back to the en value."""
    client = _build_client()
    r = client.get("/records/", params={"lang": "fr"})
    assert r.status_code == 200, r.text
    for link in r.json()["links"]:
        if "title" in link:
            # The base titles are English-only; fallback must yield a plain str.
            assert isinstance(link["title"], str)


def test_landing_page_lang_wildcard_preserves_full_dict():
    """?lang=* → the full multi-language dict is preserved on each link title."""
    client = _build_client()
    r = client.get("/records/", params={"lang": "*"})
    assert r.status_code == 200, r.text
    for link in r.json()["links"]:
        if "title" in link:
            # The Link validator wraps plain strings as {"en": "..."};
            # lang='*' must keep that dict on the wire.
            assert isinstance(link["title"], dict), (
                f"Expected dict for lang=*, got {link['title']!r}"
            )
            assert "en" in link["title"]


def test_landing_page_lang_query_conflicts_with_accept_language_returns_400():
    """?lang=en + Accept-Language: fr → 400 conflict enforced by get_language."""
    client = _build_client()
    r = client.get(
        "/records/",
        params={"lang": "en"},
        headers={"Accept-Language": "fr"},
    )
    assert r.status_code == 400, r.text
